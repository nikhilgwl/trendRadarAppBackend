import pandas as pd
import feedparser
import logging
from pytrends.request import TrendReq
from datetime import datetime
import os

logger = logging.getLogger(__name__)

# Category IDs: 242 (Skin Care), 241 (Hair Care), 234 (Makeup & Cosmetics)
BEAUTY_CATEGORIES = [242, 241, 234]
BEAUTY_KEYWORDS = ['skin', 'hair', 'makeup', 'beauty', 'serum', 'cream', 'glow', 'lipstick', 'skincare', 'haircare']

def get_google_trends():
    """Fetch beauty trends using Direct Category search with an RSS fallback."""
    trends = []
    
    # Method 1: Direct Category Search (Best data, but easily blocked)
    try:
        pytrends = TrendReq(hl='en-IN', tz=330)
        for cat_id in BEAUTY_CATEGORIES:
            pytrends.build_payload(kw_list=[''], cat=cat_id, timeframe='now 7-d', geo='IN')
            rising_data = pytrends.related_queries().get('', {}).get('rising')
            if rising_data is not None and not rising_data.empty:
                trends.extend(rising_data['query'].tolist()[:10])
    except Exception as e:
        logger.warning(f"Direct category search failed (likely blocked): {e}")

    # Method 2: RSS Fallback + Filter (Always works, but noisier)
    if not trends:
        try:
            rss_url = "https://trends.google.com/trending/rss?geo=IN"
            feed = feedparser.parse(rss_url)
            for entry in feed.entries:
                title = entry.title
                if any(kw in title.lower() for kw in BEAUTY_KEYWORDS):
                    trends.append(title)
            logger.info("Direct search failed; used Filtered RSS fallback.")
        except Exception as e:
            logger.error(f"RSS fallback failed: {e}")

    return list(set(trends))

def get_breakout_trends(keywords):
    """
    For each keyword, fetch interest over time for the last 4 hours.
    Note: This still relies on pytrends and may fail if Google blocks the request.
    """
    breakout_trends = []
    try:
        pytrends = TrendReq(hl='en-IN', tz=330)
        for kw in keywords[:5]: 
            try:
                pytrends.build_payload([kw], timeframe='now 4-H', geo='IN')
                iot = pytrends.interest_over_time()
                
                if not iot.empty and kw in iot.columns:
                    start_val = iot[kw].iloc[0]
                    end_val = iot[kw].iloc[-1]
                    if start_val < 10 and end_val > 70:
                        breakout_trends.append(kw)
            except Exception as e:
                logger.warning(f"Breakout check failed for {kw}: {e}")
    except Exception as e:
        logger.error(f"Pytrends build failed: {e}")
            
    return breakout_trends

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    trends = get_google_trends()
    print(f"Top Trends: {trends}")
