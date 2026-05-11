import feedparser
import re
from datetime import datetime, timezone, timedelta
import time
import logging

logger = logging.getLogger(__name__)

def clean_html(raw_html):
    """Strip HTML tags from summaries."""
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext.strip()

def get_rss_trends(rss_urls):
    """
    Parse RSS feeds and return articles from the last 24 hours.
    """
    all_articles = []
    now = datetime.now(timezone.utc)
    # Increased to 24 hours for better production coverage
    time_window = now - timedelta(hours=24)
    
    for url in rss_urls:
        try:
            feed = feedparser.parse(url)
            logger.info(f"Parsing RSS: {url} (Found {len(feed.entries)} total entries)")
            
            for entry in feed.entries:
                # Parse published time
                published_parsed = getattr(entry, 'published_parsed', None)
                
                # If published time is available, check window
                if published_parsed:
                    published_time = datetime.fromtimestamp(time.mktime(published_parsed), timezone.utc)
                    if published_time > time_window:
                        all_articles.append({
                            "title": entry.title,
                            "summary": clean_html(getattr(entry, 'summary', '')),
                            "link": entry.link,
                            "published": published_time.isoformat(),
                            "source": url
                        })
                else:
                    # If no timestamp, include it anyway (some feeds are like that)
                    all_articles.append({
                        "title": entry.title,
                        "summary": clean_html(getattr(entry, 'summary', '')),
                        "link": entry.link,
                        "published": now.isoformat(),
                        "source": url
                    })
        except Exception as e:
            logger.error(f"RSS fetch failed for {url}: {e}")
            
    return all_articles
