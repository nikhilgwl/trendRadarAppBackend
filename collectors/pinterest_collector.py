import logging
import feedparser
import requests
from pytrends.request import TrendReq

logger = logging.getLogger(__name__)

def get_pinterest_trends(config):
    """
    Pinterest Trends Proxy:
    Uses Google Trends or Google News RSS as fallback.
    """
    trends = []
    
    # Method 1: PyTrends (Prone to 429)
    try:
        pytrends = TrendReq(hl='en-IN', tz=330)
        inspo_queries = ["pinterest makeup look", "pinterest hair style"]
        pytrends.build_payload(inspo_queries, cat=44, timeframe='now 1-d', geo='IN')
        related = pytrends.related_queries()
        
        for q in inspo_queries:
            if q in related:
                rising = related[q].get('rising')
                if rising is not None and not rising.empty:
                    raw_trends = rising['query'].tolist()
                    for rt in raw_trends:
                        clean_trend = rt.replace('pinterest', '').strip().title()
                        if clean_trend: trends.append(clean_trend)
    except Exception as e:
        logger.warning(f"PyTrends failed for Pinterest: {e}. Falling back to RSS Discovery.")

    # Method 2: Playwright Google Search Scraping (Much more reliable)
    if not trends:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                queries = [
                    "site:pinterest.com trending makeup products india 2024",
                    "site:pinterest.com skincare ingredient trends india",
                    "site:pinterest.com haircare scalp care trends india"
                ]
                for query in queries:
                    search_url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
                    page.goto(search_url, wait_until="networkidle")
                    
                    # Extract titles and links
                    results = page.locator('h3').all()
                    for res in results[:5]:
                        title = res.inner_text()
                        parent = res.locator('xpath=..') # Get parent anchor
                        link = parent.get_attribute('href')
                        
                        clean = title.split('|')[0].split('-')[0].strip()
                        if clean and len(clean) > 8:
                            # If it's a trend name, we might want to store it as a dict
                            trends.append({"title": clean, "url": link or search_url})
                browser.close()
        except Exception as e:
            logger.warning(f"Playwright fallback failed for Pinterest: {e}")

    # Method 3: Google News RSS Discovery (Legacy fallback)
    if not trends:
        try:
            search_query = "site:pinterest.com beauty looks india when:7d"
            url = f"https://news.google.com/rss/search?q={search_query.replace(' ', '+')}&hl=en-IN&gl=IN&ceid=IN:en"
            r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            d = feedparser.parse(r.content)
            for entry in d.entries[:10]:
                title = entry.title.split('-')[0].strip()
                if title: trends.append({"title": title, "url": entry.link})
        except Exception as e:
            logger.error(f"RSS fallback failed for Pinterest: {e}")

    # Final Fallback: Curated Aesthetic Themes (Only if EVERYTHING else fails)
    if not trends:
        fallback_titles = ["90s Blowout Hair", "Monochromatic Peach Makeup", "Scalp Slugging", "Glazed Skin"]
        trends = [{"title": t, "url": ""} for t in fallback_titles]

    return trends
