"""
Amazon India beauty bestseller collector.
Refactored to use curl_cffi for direct HTML scraping (bypasses CAPTCHAs).
No Playwright fallback — fetches actual bestseller data from Amazon.
"""
import logging
import time
import random
from curl_cffi import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

AMAZON_CATEGORIES = [
    {"name": "Beauty",    "url": "https://www.amazon.in/gp/bestsellers/beauty/"},
    {"name": "Skincare",  "url": "https://www.amazon.in/gp/bestsellers/beauty/1355016031/"},
    {"name": "Haircare",  "url": "https://www.amazon.in/gp/bestsellers/beauty/1355015031/ref=zg_bs_nav_beauty_2_lfb_j9v"},
    {"name": "Makeup",    "url": "https://www.amazon.in/gp/bestsellers/beauty/1355014031/ref=zg_bs_nav_beauty_2_lfb_j9v"},
]

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
}

def _parse_html(html: str, category: str) -> list:
    """Extract granular product info from Amazon bestseller HTML."""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Amazon bestseller grid items
    for item in soup.select("div#gridItemRoot"):
        try:
            # 1. Rank
            rank_el = item.select_one("span.zg-bdg-text")
            rank_text = rank_el.text.strip().lstrip("#") if rank_el else "99"
            rank = int(rank_text) if rank_text.isdigit() else 99

            # 2. Name & Brand (Amazon often combines them)
            name_el = item.select_one("div[class*='p13n-sc-css-line-clamp']")
            if not name_el:
                name_el = item.select_one("span div")
            
            product_name = name_el.text.strip() if name_el else "Unknown Product"
            
            # 3. Image
            img_el = item.select_one("img")
            img_url = img_el.get("src") if img_el else None

            # 4. Price
            price_el = item.select_one("span.p13n-sc-price")
            price = price_el.text.strip() if price_el else None

            # 5. Rating & Reviews
            rating_el = item.select_one("span.a-icon-alt")
            rating = rating_el.text.strip().split(" ")[0] if rating_el else None
            
            review_count_el = item.select_one("span.a-size-small") # Usually reviews follow stars
            reviews = review_count_el.text.strip() if review_count_el else None

            # 6. ASIN (optional but good for uniqueness)
            asin = None
            parent = item.select_one("div[data-asin]")
            if parent:
                asin = parent.get("data-asin")

            results.append({
                "rank":         rank,
                "product_name": product_name,
                "asin":         asin,
                "category":     category,
                "price":        price,
                "rating":       rating,
                "reviews":      reviews,
                "image_url":    img_url,
                "source":       "Amazon India Bestsellers",
            })
        except Exception:
            continue

    return results

def get_amazon_trends(config=None) -> list:
    """
    Systematically crawl Amazon India beauty bestsellers.
    1. Start at Beauty root.
    2. Discover subcategory links.
    3. Crawl Skincare, Haircare, Makeup.
    """
    all_products = []
    
    # Root beauty URL
    beauty_root = "https://www.amazon.in/gp/bestsellers/beauty/"
    
    try:
        logger.info("Amazon: Systematic crawl started at Beauty root...")
        resp = requests.get(beauty_root, headers=_HEADERS, impersonate="chrome", timeout=30)
        if resp.status_code != 200:
            logger.error(f"Amazon root failed: {resp.status_code}")
            return []

        # Parse root products
        all_products.extend(_parse_html(resp.text, "Beauty")[:15])
        
        # Discover subcategory links
        soup = BeautifulSoup(resp.text, "html.parser")
        subcat_map = {
            "Skincare": ["skincare", "skin care"],
            "Haircare": ["haircare", "hair care"],
            "Makeup":   ["makeup", "make-up"]
        }
        
        found_links = {}
        for a in soup.select("a"):
            href = a.get("href", "")
            text = a.text.lower()
            if "/gp/bestsellers/beauty/" in href:
                for cat_name, keywords in subcat_map.items():
                    if any(kw in text for kw in keywords):
                        # Ensure absolute URL
                        if href.startswith("/"):
                            href = "https://www.amazon.in" + href
                        found_links[cat_name] = href

        # Crawl discovered subcategories one by one with delays
        for cat_name, url in found_links.items():
            logger.info(f"Amazon: Crawling subcategory {cat_name} -> {url}")
            time.sleep(random.uniform(5, 10)) # Human-like pause
            
            c_resp = requests.get(url, headers=_HEADERS, impersonate="chrome", timeout=30)
            if c_resp.status_code == 200:
                products = _parse_html(c_resp.text, cat_name)
                logger.info(f"Amazon {cat_name}: {len(products)} products found")
                all_products.extend(products[:15])
            else:
                logger.warning(f"Amazon {cat_name} failed: {c_resp.status_code}")

    except Exception as e:
        logger.error(f"Amazon systematic crawl error: {e}")

    logger.info(f"Amazon systematic crawl complete: {len(all_products)} products total")
    return all_products

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    res = get_amazon_trends()
    for r in res[:10]:
        print(f"#{r['rank']} [{r['category']}] {r['product_name']}")
