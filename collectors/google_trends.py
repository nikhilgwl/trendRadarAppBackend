import pandas as pd
import feedparser
import logging
from pytrends.request import TrendReq
from datetime import datetime
import os

logger = logging.getLogger(__name__)

# Category IDs: 242 (Skin Care), 241 (Hair Care), 234 (Makeup & Cosmetics)
BEAUTY_CATEGORIES = [242, 241, 234]

def get_google_trends():
    """Directly fetch 'Rising' beauty trends using Google Category IDs."""
    direct_trends = []
    try:
        pytrends = TrendReq(hl='en-IN', tz=330)
        
        for cat_id in BEAUTY_CATEGORIES:
            try:
                # We search for 'Rising' queries in India for these categories over the last 7 days
                pytrends.build_payload(kw_list=[''], cat=cat_id, timeframe='now 7-d', geo='IN')
                related_queries = pytrends.related_queries()
                
                # 'rising' queries are those with the highest growth percentage
                rising_data = related_queries.get('', {}).get('rising')
                
                if rising_data is not None and not rising_data.empty:
                    # Extract the query strings
                    queries = rising_data['query'].tolist()
                    direct_trends.extend(queries[:10]) # Top 10 from each category
                    logger.info(f"Fetched {len(queries)} direct trends for category {cat_id}")
            except Exception as e:
                logger.warning(f"Failed to fetch for category {cat_id}: {e}")
                
    except Exception as e:
        logger.error(f"Pytrends initialization failed: {e}")

    # Fallback to general RSS if direct search fails (with filter)
    if not direct_trends:
        return [] # Or use the previous filtering logic as a backup

    return list(set(direct_trends))

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
