import feedparser
import logging

logger = logging.getLogger(__name__)

BEAUTY_YOUTUBE_CHANNELS = {
    'BeBeautiful': 'UC9OGv_UQ1JCbz5IDIQiLpAg',
    'Nykaa': 'UCywSjzALmEHrYXB4GYqxnlA',
    'Shruti Arjun Anand': 'UCBg8GrJ5MQfE9OqiYYMdEDQ',
}

def get_social_trends(config=None):
    """
    Fetch trending beauty videos from top Indian YouTube channels via RSS.
    Replaces old Twitter scrape.
    """
    trends = []
    
    for name, channel_id in BEAUTY_YOUTUBE_CHANNELS.items():
        try:
            url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            feed = feedparser.parse(url)
            for entry in feed.entries[:3]:
                title = getattr(entry, 'title', '').strip()
                if title:
                    trends.append(title)
            logger.info(f"Fetched {len(feed.entries[:3])} videos from YouTube channel {name}")
        except Exception as e:
            logger.error(f"Error fetching YouTube RSS for {name}: {e}")
            
    return list(set(trends))
