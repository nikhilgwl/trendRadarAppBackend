"""
Pinterest beauty trend collector.

Method 1: Playwright on Pinterest Ideas/Explore pages (beauty-specific curated content)
Method 2: Playwright on Pinterest search for India beauty terms
Method 3: Google News RSS fallback
"""
import logging
import random
import time
import feedparser
import requests

logger = logging.getLogger(__name__)

_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-IN,en;q=0.9',
}

# Pinterest curated idea pages — beauty-relevant
PINTEREST_IDEAS_PAGES = [
    {'url': 'https://www.pinterest.com/ideas/beauty-tips/',       'cat': 'Beauty'},
    {'url': 'https://www.pinterest.com/ideas/skin-care/',         'cat': 'Skincare'},
    {'url': 'https://www.pinterest.com/ideas/hair-care/',         'cat': 'Haircare'},
    {'url': 'https://www.pinterest.com/ideas/makeup-looks/',      'cat': 'Makeup'},
]

PINTEREST_SEARCH_TERMS = [
    'skincare routine india',
    'hair care tips india',
    'makeup look india',
]

RSS_FALLBACK_QUERIES = [
    'pinterest+skincare+trend+india',
    'pinterest+makeup+look+india',
    'pinterest+hair+trend+india',
]


def _parse_pinterest_page(html: str, cat: str) -> list:
    """Extract pin titles and board names from a Pinterest page HTML."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    seen = set()

    # Pin titles — Pinterest uses various selectors
    for sel in [
        'div[data-test-id="pinTitle"] span',
        'div[data-test-id="pin-visual-wrapper"] ~ div span',
        'h3[data-test-id="product-card-title"]',
        'div[role="listitem"] div[style*="font-weight: bold"]',
        'span[style*="font-weight: bold"]',
        'div[class*="PinTitle"] span',
        'a[data-test-id="pin-link"]',
        'div[class*="boardRepTitle"]',
        'div[class*="Title"] span',
        'span[class*="title"]',
    ]:
        for el in soup.select(sel)[:30]:
            text = el.get_text(strip=True)
            if text and len(text) > 5 and text.lower() not in seen:
                seen.add(text.lower())
                results.append({'title': text, 'category': cat, 'url': ''})

    # If dedicated selectors fail, try generic link text with length filter
    if not results:
        for a in soup.find_all('a', href=True)[:50]:
            text = a.get_text(strip=True)
            href = a.get('href', '')
            if (text and 8 < len(text) < 100
                    and ('/pin/' in href or '/ideas/' in href)
                    and text.lower() not in seen):
                seen.add(text.lower())
                results.append({'title': text, 'category': cat, 'url': f"https://www.pinterest.com{href}"})

    return results[:15]


def _fetch_pinterest_playwright(pages: list) -> list:
    """Scrape Pinterest ideas/search pages using Playwright."""
    results = []
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=_HEADERS['User-Agent'],
                viewport={'width': 1440, 'height': 900},
                locale='en-US',
                timezone_id='Asia/Kolkata',
            )
            page = ctx.new_page()
            page.route('**/*.{mp4,webp,gif,woff,woff2,ttf,otf,ico}', lambda r: r.abort())

            for item in pages:
                try:
                    url = item['url']
                    cat = item.get('cat', 'Beauty')
                    logger.info(f"Pinterest Playwright: {url}")
                    page.goto(url, timeout=25000, wait_until='domcontentloaded')
                    time.sleep(random.uniform(2.5, 4.0))

                    # Scroll to load more pins
                    page.evaluate('window.scrollBy(0, 1200)')
                    time.sleep(1.5)
                    page.evaluate('window.scrollBy(0, 1200)')
                    time.sleep(1.0)

                    html = page.content()
                    parsed = _parse_pinterest_page(html, cat)
                    results.extend(parsed)
                    logger.info(f"Pinterest {cat}: {len(parsed)} pins found")
                    time.sleep(random.uniform(2, 3))
                except Exception as e:
                    logger.warning(f"Pinterest page {item['url']} failed: {e}")

            browser.close()
    except Exception as e:
        logger.warning(f"Pinterest Playwright session failed: {e}")
    return results


def _fetch_pinterest_search_playwright(terms: list) -> list:
    """Search Pinterest for specific India beauty terms."""
    results = []
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=_HEADERS['User-Agent'],
                viewport={'width': 1440, 'height': 900},
                locale='en-US',
            )
            page = ctx.new_page()
            page.route('**/*.{mp4,webp,gif,woff,woff2,ttf,ico}', lambda r: r.abort())

            for term in terms:
                try:
                    q = term.replace(' ', '%20')
                    page.goto(
                        f"https://www.pinterest.com/search/pins/?q={q}&rs=typed",
                        timeout=25000, wait_until='domcontentloaded'
                    )
                    time.sleep(random.uniform(2.5, 4.0))
                    html = page.content()
                    parsed = _parse_pinterest_page(html, 'India Beauty')
                    results.extend(parsed)
                    logger.info(f"Pinterest search '{term}': {len(parsed)} results")
                    time.sleep(random.uniform(2, 3))
                except Exception as e:
                    logger.warning(f"Pinterest search '{term}' failed: {e}")

            browser.close()
    except Exception as e:
        logger.warning(f"Pinterest search Playwright failed: {e}")
    return results


def _fetch_rss_fallback() -> list:
    """Google News RSS as final fallback for Pinterest beauty signals."""
    results = []
    try:
        for q in RSS_FALLBACK_QUERIES:
            url = f"https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"
            feed = feedparser.parse(url)
            for entry in feed.entries[:4]:
                title = getattr(entry, 'title', '').strip()
                link = getattr(entry, 'link', '')
                if title:
                    clean = title.split(' - ')[0].split(' | ')[0].strip()
                    if clean and len(clean) > 8:
                        results.append({'title': clean, 'url': link, 'category': 'Beauty'})
    except Exception as e:
        logger.warning(f"Pinterest RSS fallback failed: {e}")
    logger.info(f"Pinterest RSS fallback: {len(results)} entries")
    return results[:10]


def get_pinterest_trends(config=None) -> list:
    """
    Collect Pinterest beauty trend signals.
    Returns list of {title, url, category} dicts.
    """
    results: list = []
    seen: set = set()

    def add(items):
        for item in items:
            title = (item.get('title') or '').strip()
            key = title.lower()[:50]
            if title and key not in seen:
                seen.add(key)
                results.append(item)

    # Method 1: Playwright on curated Pinterest ideas pages (sample 2 categories)
    sampled_pages = random.sample(PINTEREST_IDEAS_PAGES, min(2, len(PINTEREST_IDEAS_PAGES)))
    add(_fetch_pinterest_playwright(sampled_pages))
    logger.info(f"Pinterest after ideas pages: {len(results)}")

    # Method 2: Playwright search for India beauty terms — if still sparse
    if len(results) < 8:
        add(_fetch_pinterest_search_playwright(PINTEREST_SEARCH_TERMS[:2]))

    # Method 3: RSS fallback
    if len(results) < 5:
        add(_fetch_rss_fallback())

    # Always add a few RSS items for freshness
    if len(results) < 15:
        add(_fetch_rss_fallback())

    logger.info(f"Pinterest collector final: {len(results)} trends")
    return results[:20]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    results = get_pinterest_trends()
    print(f"\n{len(results)} Pinterest trends:")
    for r in results[:10]:
        print(f"  [{r.get('category')}] {r.get('title', '')[:80]}")
