import requests
from bs4 import BeautifulSoup
import logging
from pytrends.request import TrendReq

logger = logging.getLogger(__name__)

def get_social_trends(config):
    """
    Combined social trend fetcher with multi-layer fallback.
    """
    trends = []
    
    # 1. Twitter India Trends (Scrape)
    try:
        url = "https://getdaytrends.com/india/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            trend_elements = soup.select('td.main a')
            all_hashtags = [el.text.strip() for el in trend_elements]
            trends.extend(all_hashtags[:10])
            logger.info(f"Fetched {len(all_hashtags)} hashtags from Twitter India.")
    except Exception as e:
        logger.error(f"Error fetching Twitter trends: {e}")

    # 2. Google Trends (API - Prone to 429/404)
    try:
        # Use a fresh request object each time to avoid 404 session issues
        pytrends = TrendReq(hl='en-IN', tz=330, retries=2, backoff_factor=0.1)
        trending_searches = pytrends.trending_searches(pn='india')
        if not trending_searches.empty:
            trends.extend(trending_searches[0].tolist()[:10])
            logger.info("Fetched top 10 trending searches from Google India.")
    except Exception as e:
        logger.warning(f"Google Trends API failed: {e}. Moving on.")
        
    return list(set(trends))
