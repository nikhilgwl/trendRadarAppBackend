import feedparser
import re
from datetime import datetime, timezone, timedelta
import time
import logging

logger = logging.getLogger(__name__)

# Fix 7: beauty relevance filter — same pattern as other collectors
_BEAUTY_KEYWORDS = {
    'skin', 'hair', 'makeup', 'beauty', 'serum', 'cream', 'glow', 'lip',
    'skincare', 'haircare', 'sunscreen', 'moisturizer', 'shampoo', 'conditioner',
    'retinol', 'niacinamide', 'vitamin', 'toner', 'face', 'foundation',
    'kajal', 'kohl', 'blush', 'nail', 'scalp', 'dandruff', 'spf', 'acne',
    'brightening', 'pigmentation', 'keratin', 'argan', 'biotin', 'collagen',
    'hyaluronic', 'ceramide', 'peptide', 'glycolic', 'salicylic', 'kojic',
    'cleanser', 'exfoliant', 'tret', 'aha', 'bha', 'derma', 'ingredient',
    'fragrance', 'perfume', 'lotion', 'balm', 'mask', 'pore', 'hydrat',
    'mamaearth', 'minimalist', 'plum', 'lakme', 'dove', 'tresemme',
    'indulekha', 'pond', 'vaseline', 'simple', 'wow skin',
}

_EXCLUDE_KEYWORDS = {
    'cricket', 'ipl', 'match', 'election', 'vote', 'minister', 'politics',
    'stock', 'share price', 'nifty', 'sensex', 'gold price', 'weather',
    'earthquake', 'accident', 'death', 'killed', 'modi', 'bjp', 'congress',
    'movie', 'actor', 'trailer', 'song', 'court', 'police',
}


def _is_beauty_relevant(title: str, summary: str = "") -> bool:
    """Return True only if the article is clearly beauty-related."""
    text = (title + " " + summary).lower()
    if any(ex in text for ex in _EXCLUDE_KEYWORDS):
        return False
    return any(kw in text for kw in _BEAUTY_KEYWORDS)


def clean_html(raw_html):
    """Strip HTML tags from summaries."""
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext.strip()


def get_rss_trends(rss_urls):
    """
    Parse RSS feeds and return beauty-relevant articles from the last 24 hours.
    Fix 7: articles are filtered for beauty relevance before being returned,
    so Gemini's NEWS section only contains on-topic content.
    """
    all_articles = []
    now = datetime.now(timezone.utc)
    time_window = now - timedelta(hours=24)

    for url in rss_urls:
        try:
            feed = feedparser.parse(url)
            logger.info(f"RSS: {url} — {len(feed.entries)} entries")

            for entry in feed.entries:
                title = getattr(entry, 'title', '')
                summary = clean_html(getattr(entry, 'summary', ''))

                # Fix 7: skip non-beauty articles early
                if not _is_beauty_relevant(title, summary):
                    continue

                published_parsed = getattr(entry, 'published_parsed', None)
                if published_parsed:
                    published_time = datetime.fromtimestamp(time.mktime(published_parsed), timezone.utc)
                    if published_time <= time_window:
                        continue
                else:
                    published_time = now

                all_articles.append({
                    "title": title,
                    "summary": summary,
                    "link": entry.link,
                    "published": published_time.isoformat(),
                    "source": url,
                })
        except Exception as e:
            logger.error(f"RSS fetch failed for {url}: {e}")

    logger.info(f"RSS collector: {len(all_articles)} beauty-relevant articles")
    return all_articles
