"""
Nykaa beauty trend collector.
Refactored to use curl_cffi for high-precision HTML scraping (bypasses Datadome).
No Playwright or RSS fallbacks — fetches actual product data from Nykaa.
"""
import logging
import random
import time
from curl_cffi import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Verified working SSR URLs (return actual product data without JS execution).
# Nykaa's new-format SPA pages (/makeup/c/1369 etc.) require JS and are skipped.
NYKAA_PAGES = [
    {'name': 'Skincare Bestsellers',    'url': 'https://www.nykaa.com/beauty/skin-care/c/2?sort=popularity&page_no=1', 'category': 'Skincare'},
    {'name': 'Skincare Bestsellers p2', 'url': 'https://www.nykaa.com/beauty/skin-care/c/2?sort=popularity&page_no=2', 'category': 'Skincare'},
    {'name': 'Lips Bestsellers',        'url': 'https://www.nykaa.com/beauty/lips/c/12?sort=popularity',               'category': 'Makeup'},
    {'name': 'Lipstick Bestsellers',    'url': 'https://www.nykaa.com/beauty/lipstick/c/30?sort=popularity',           'category': 'Makeup'},
    {'name': 'Shampoo Bestsellers',     'url': 'https://www.nykaa.com/beauty/shampoo/c/20?sort=popularity',            'category': 'Haircare'},
    {'name': 'Hair Oils Bestsellers',   'url': 'https://www.nykaa.com/beauty/hair-oils/c/21?sort=popularity',          'category': 'Haircare'},
    {'name': 'Conditioner Bestsellers', 'url': 'https://www.nykaa.com/beauty/conditioner/c/22?sort=popularity',        'category': 'Haircare'},
    {'name': 'Hair Colour Bestsellers', 'url': 'https://www.nykaa.com/beauty/hair-colour/c/40?sort=popularity',        'category': 'Haircare'},
]

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nykaa.com/',
}

import re as _re

_SLUG_CATEGORY_SIGNALS = {
    'Skincare': {'serum', 'moisturizer', 'moisturiser', 'sunscreen', 'spf', 'face-wash',
                 'toner', 'cleanser', 'exfoliant', 'retinol', 'niacinamide', 'vitamin-c',
                 'face-oil', 'face-mask', 'sheet-mask', 'eye-cream', 'under-eye', 'acne',
                 'brightening', 'kojic', 'glycolic', 'salicylic', 'hyaluronic', 'ceramide'},
    'Haircare': {'shampoo', 'conditioner', 'hair-oil', 'hair-serum', 'hair-mask', 'hair-colour',
                 'hair-color', 'hair-growth', 'scalp', 'dandruff', 'keratin', 'onion',
                 'argan', 'biotin', 'rosemary', 'hair-spray', 'heat-protectant'},
    'Makeup':   {'lipstick', 'lip-gloss', 'lip-liner', 'foundation', 'concealer', 'kajal',
                 'kohl', 'mascara', 'eyeliner', 'blush', 'bronzer', 'highlighter', 'primer',
                 'setting-spray', 'contour', 'bb-cream', 'cc-cream', 'nail-polish'},
}

def _infer_category_from_slug(slug: str, page_category: str) -> str:
    """Return category inferred from product URL slug. Falls back to page_category."""
    slug_lower = slug.lower()
    for cat, signals in _SLUG_CATEGORY_SIGNALS.items():
        if any(sig in slug_lower for sig in signals):
            return cat
    return page_category


def _clean_product_name(raw: str) -> str:
    """Strip Nykaa UI chrome from product names (AD, BESTSELLER labels, price/discount noise)."""
    name = raw.strip()
    # Remove leading labels
    for prefix in ('BESTSELLER', 'AD', 'NEW', 'HOT', 'SALE'):
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
    # Strip trailing price/discount junk: e.g. "?499?35030% Off..." or "₹499₹350"
    name = _re.split(r'[?₹\d][\d?₹%\s]*(Off|off|\bMRP\b)', name)[0].strip()
    # Strip trailing price numbers: "Name?499" → "Name"
    name = _re.split(r'[?₹]\d{2,5}', name)[0].strip()
    # Strip "Regular price..." suffix
    name = name.split('Regular price')[0].strip()
    return name


def _parse_nykaa_html(html: str, category: str) -> list:
    """
    Extract product names from Nykaa page HTML (SSR format).
    Uses href slugs from /p/ links — the slug is always the canonical product name
    and is free of price/discount noise. img alt tags are used as display label.
    """
    soup = BeautifulSoup(html, 'html.parser')
    products = []
    seen = set()

    for a in soup.select('a[href*="/p/"]'):
        href = a.get('href', '')
        # Slug is between start and /p/<id> — e.g. /l-oreal-paris-serum/p/12345
        slug_match = _re.match(r'^/?([^/].+)/p/\d+', href)
        if not slug_match:
            continue
        slug = slug_match.group(1).split('/')[-1]  # last path segment
        # Convert hyphen-slug to title case readable name
        name_from_slug = slug.replace('-', ' ').title()

        # Use img alt as the display name if it's clean (no price noise)
        img = a.find('img')
        alt = img.get('alt', '').strip() if img else ''
        display_name = alt if (alt and len(alt) > 8 and len(alt) < 120
                               and not any(c in alt for c in ['?', '₹', '%'])) else name_from_slug

        key = slug[:60]
        if key in seen or len(display_name) < 5:
            continue
        seen.add(key)
        # Infer category from slug (more accurate than page category due to cross-promo ads)
        inferred_cat = _infer_category_from_slug(slug, category)
        products.append({'product_name': display_name, 'category': inferred_cat, 'source': 'Nykaa'})

    return products[:15]

def get_nykaa_trends(config=None) -> list:
    """
    Collect India beauty commerce trends directly from Nykaa HTML.
    Uses curl_cffi to bypass Datadome protection.
    Always includes at least one page from each major category.
    """
    results = []
    # Guarantee at least one page per category, then sample remaining
    by_cat: dict = {}
    for pg in NYKAA_PAGES:
        by_cat.setdefault(pg['category'], []).append(pg)

    pages = []
    remaining = []
    for cat_pages in by_cat.values():
        chosen = random.choice(cat_pages)
        pages.append(chosen)
        remaining.extend([p for p in cat_pages if p is not chosen])

    # Add up to 2 more from remaining to get ~5 total
    if remaining:
        pages.extend(random.sample(remaining, min(2, len(remaining))))
    random.shuffle(pages)

    for p_info in pages:
        try:
            logger.info(f"Fetching Nykaa: {p_info['name']}")
            resp = requests.get(p_info['url'], headers=_HEADERS, impersonate="chrome", timeout=30)
            
            if resp.status_code == 200:
                parsed = _parse_nykaa_html(resp.text, p_info['category'])
                results.extend(parsed)
                logger.info(f"Nykaa {p_info['name']}: {len(parsed)} products")
            else:
                logger.warning(f"Nykaa {p_info['name']} returned HTTP {resp.status_code}")
            
            time.sleep(random.uniform(2, 4))
        except Exception as e:
            logger.error(f"Nykaa fetch failed for {p_info['name']}: {e}")

    logger.info(f"Nykaa collector total: {len(results)} products")
    return results

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    res = get_nykaa_trends()
    for r in res[:10]:
        print(f"[{r['category']}] {r['product_name']}")
