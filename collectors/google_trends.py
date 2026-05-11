import pandas as pd
import feedparser
import logging
from pytrends.request import TrendReq
from datetime import datetime
import os

logger = logging.getLogger(__name__)

def get_google_trends():
    """Fetch top trending searches in India using RSS as primary and pytrends as fallback."""
    trends = []
    
    # Method 1: RSS Feed (Most reliable currently)
    try:
        rss_url = "https://trends.google.com/trending/rss?geo=IN"
        feed = feedparser.parse(rss_url)
        if feed.entries:
            for entry in feed.entries:
                trends.append(entry.title)
            logger.info(f"Fetched {len(trends)} trends via RSS.")
    except Exception as e:
        logger.warning(f"RSS trends fetch failed: {e}")

    # Method 2: Pytrends (Fallback, though often 404s recently)
    if not trends:
        try:
            pytrends = TrendReq(hl='en-IN', tz=330)
            trending_searches = pytrends.trending_searches(pn='IN')
            if not trending_searches.empty:
                trends.extend(trending_searches[0].tolist()[:20])
                logger.info(f"Fetched {len(trends)} trends via Pytrends.")
        except Exception as e:
            logger.warning(f"Pytrends fetch failed: {e}")
            
    return list(set(trends)) # Deduplicated

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
