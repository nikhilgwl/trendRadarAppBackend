import pandas as pd
import feedparser
import logging
from pytrends.request import TrendReq
from datetime import datetime
import os

logger = logging.getLogger(__name__)

BEAUTY_KEYWORDS = [
    'skin', 'hair', 'makeup', 'beauty', 'serum', 'cream', 'oil', 'shampoo', 
    'lipstick', 'foundation', 'glow', 'acne', 'vitamin', 'acid', 'moisturizer',
    'routine', 'facial', 'derma', 'cosmetic', 'loreal', 'lakme', 'nykaa',
    'skincare', 'haircare', 'wellness', 'spa', 'retinol', 'niacinamide'
]

def get_google_trends():
    """Fetch trending searches in India and filter for beauty relevance."""
    all_trends = []
    
    # Method 1: RSS Feed
    try:
        rss_url = "https://trends.google.com/trending/rss?geo=IN"
        feed = feedparser.parse(rss_url)
        if feed.entries:
            for entry in feed.entries:
                all_trends.append(entry.title)
    except Exception as e:
        logger.warning(f"RSS trends fetch failed: {e}")

    # Filter for beauty relevance
    beauty_trends = []
    for trend in all_trends:
        trend_lower = trend.lower()
        if any(kw in trend_lower for kw in BEAUTY_KEYWORDS):
            beauty_trends.append(trend)
            
    logger.info(f"Filtered {len(beauty_trends)} beauty trends out of {len(all_trends)} total searches.")
    return list(set(beauty_trends))

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
