"""
Google Trends collector — multi-method chain.

Method 1 & 2: Google Trends Realtime RSS (actual trending searches, India)
              Free endpoint, no API key, no pytrends, minimal rate-limiting.
Method 3:     Google Daily Trends JSON API (with traffic volumes)
Method 4:     pytrends related_queries() on beauty seeds — rising/breakout queries
Method 5:     pytrends interest_over_time() for ingredient keywords
Method 6:     Google News RSS fallback
"""
import json
import random
import time
import logging
import os
import re
import feedparser
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

BEAUTY_FILTER = [
    'skin', 'hair', 'makeup', 'beauty', 'serum', 'cream', 'glow', 'lip',
    'skincare', 'haircare', 'sunscreen', 'moisturizer', 'shampoo', 'conditioner',
    'retinol', 'niacinamide', 'vitamin c', 'toner', 'face wash', 'foundation',
    'kajal', 'kohl', 'blush', 'bronzer', 'primer', 'mascara', 'nail', 'scalp',
    'dandruff', 'hair fall', 'hair oil', 'face pack', 'mask', 'spf', 'cleanser',
    'exfoliant', 'hyaluronic', 'ceramide', 'peptide', 'collagen', 'acne', 'pore',
    'brightening', 'pigmentation', 'tan', 'de-tan', 'eye cream', 'under eye',
    'rosemary', 'onion oil', 'argan', 'biotin', 'keratin', 'hair growth',
    'salicylic', 'glycolic', 'kojic', 'body lotion', 'face oil', 'bb cream',
    'cc cream', 'concealer', 'contour', 'highlighter', 'setting spray', 'deodorant',
    'fragrance', 'perfume', 'scrub', 'exfoliat', 'tret', 'aha', 'bha', 'pha',
    'derma', 'dermatol', 'ingredient', 'skincare routine', 'haircare routine',
    'balm', 'gloss', 'lipstick', 'liner', 'manicure', 'pedicure', 'spa', 'facelift',
    'botox', 'filler', 'waxing', 'epilat', 'shave', 'trimmer', 'beard',
]

BROAD_SEEDS = ['skincare', 'haircare', 'makeup', 'face serum', 'hair care routine']
INGREDIENT_SEEDS = [
    'vitamin c serum', 'niacinamide', 'retinol', 'hyaluronic acid', 'salicylic acid'
]
NEWS_RSS_QUERIES = [
    'skincare+trends+india',
    'beauty+product+launch+india',
    'haircare+india+new',
    'makeup+trends+india',
    'skincare+ingredients+india',
]

_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-IN,en;q=0.9',
    'Accept': 'application/rss+xml, application/xml, text/xml, */*',
}


EXCLUDE_FILTER = [
    'cricket', 'ipl', 'match', 'score', 'team', 'player', 'election', 'vote',
    'minister', 'politics', 'stadium', 'ball', 'wicket', 't20', 'odi', 'test',
    'world cup', 'stock', 'share price', 'nifty', 'sensex', 'market', 'gold price',
    'weather', 'rain', 'cyclone', 'earthquake', 'accident', 'death', 'killed',
    'hardik pandya', 'sunil gavaskar', 'virat kohli', 'dhoni', 'rohit sharma',
    'pune', 'mumbai', 'delhi', 'bangalore', 'chennai', 'hyderabad', 'kolkata',
    'news', 'live', 'today', 'movie', 'actor', 'actress', 'trailer', 'song',
    'modi', 'rahul gandhi', 'bjp', 'congress', 'government', 'police', 'court',
]


def _is_beauty(text: str) -> bool:
    t = text.lower()
    if any(ex in t for ex in EXCLUDE_FILTER):
        return False
    return any(kw in t for kw in BEAUTY_FILTER)


# ──────────────────────────────────────────
# Method 1 & 2: Google Trends Realtime RSS
# ──────────────────────────────────────────

def _fetch_trends_rss(hours: int = 48, cat: str = '') -> list:
    """
    Actual Google Trends trending searches for India — not news articles.
    Returns list of dicts with query, traffic, articles_count.
    This endpoint is public, free, and far less rate-limited than pytrends.
    """
    results = []
    try:
        url = f"https://trends.google.com/trending/rss?geo=IN&hl=en-IN&hours={hours}"
        if cat:
            url += f"&cat={cat}"

        resp = requests.get(url, headers=_HEADERS, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"Trends RSS returned HTTP {resp.status_code}")
            return []

        feed = feedparser.parse(resp.content)
        for entry in feed.entries:
            title = getattr(entry, 'title', '').strip()
            traffic = getattr(entry, 'ht_approx_traffic', '') or ''
            if not title:
                continue
            if not _is_beauty(title):
                continue
            results.append({
                'query':   title,
                'traffic': str(traffic).strip(),
                'source':  f'Google Trends RSS (hours={hours})',
            })

        label = f"cat={cat}" if cat else "all-filtered"
        logger.info(f"Google Trends RSS ({label}, hours={hours}): {len(results)} beauty trends from {len(feed.entries)} total")
    except Exception as e:
        logger.warning(f"Google Trends RSS failed: {e}")
    return results


# ──────────────────────────────────────────
# Method 3: Wider RSS windows for more coverage
# ──────────────────────────────────────────

def _fetch_wider_rss() -> list:
    """
    Additional RSS calls with longer time windows to capture more beauty trends.
    Google's public Trends RSS has no rate limiting issues.
    """
    results = []
    # 24h beauty+fashion category window
    results.extend(_fetch_trends_rss(hours=24, cat='44'))
    time.sleep(random.uniform(1.0, 2.0))
    # 48h general (beauty-filtered) for any breakout
    results.extend(_fetch_trends_rss(hours=72))
    return results


# ──────────────────────────────────────────
# Method 4: pytrends breakout / rising queries
# ──────────────────────────────────────────

def _make_pytrends():
    """Build pytrends with cloudscraper UA for better bypass."""
    from pytrends.request import TrendReq
    try:
        import cloudscraper
        cs = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        ua = cs.headers.get('User-Agent', _HEADERS['User-Agent'])
    except ImportError:
        ua = _HEADERS['User-Agent']

    return TrendReq(
        hl='en-IN', tz=330, timeout=(15, 45), retries=2, backoff_factor=1.5,
        requests_args={
            'headers': {
                'User-Agent': ua,
                'Accept-Language': 'en-IN,en;q=0.9',
                'Cookie': (
                    'CONSENT=YES+cb.20210328-17-p0.en-GB+FX+719; '
                    'SOCS=CAESHAgCEhJnd3NfMjAyMzEwMTktMF9SQzEaAmVuIAEaBgiA0KCnBg;'
                ),
            }
        }
    )


def _fetch_pytrends_rising(seeds: list, cat: int = 0) -> list:
    """
    pytrends related_queries() — rising / breakout queries.
    cat=44 = Beauty & Fashion, cat=0 = all categories.
    """
    results = []
    try:
        pt = _make_pytrends()
        time.sleep(random.uniform(3, 6))
        pt.build_payload(seeds[:5], timeframe='now 7-d', geo='IN', cat=cat)
        related = pt.related_queries()
        for seed in seeds[:5]:
            rising_df = (related.get(seed) or {}).get('rising')
            if rising_df is not None and not rising_df.empty:
                for _, row in rising_df.iterrows():
                    q = str(row.get('query', '')).strip()
                    if q and (cat == 44 or _is_beauty(q)):
                        results.append({
                            'query':  q,
                            'traffic': f"+{row.get('value', '')}%",
                            'source': f'pytrends rising (cat={cat})',
                        })
        logger.info(f"pytrends rising (cat={cat}): {len(results)} breakout queries")
        time.sleep(random.uniform(4, 8))
    except Exception as e:
        logger.warning(f"pytrends rising queries failed (cat={cat}): {e}")
    return results


def _fetch_pytrends_iot(keywords: list) -> list:
    """
    pytrends interest_over_time — detect rising ingredient keywords.
    """
    results = []
    try:
        pt = _make_pytrends()
        time.sleep(random.uniform(3, 6))
        pt.build_payload(keywords[:5], timeframe='now 7-d', geo='IN')
        iot = pt.interest_over_time()
        if not iot.empty:
            for kw in keywords[:5]:
                if kw in iot.columns:
                    series = iot[kw]
                    if len(series) >= 2 and series.iloc[-1] > series.iloc[0] * 1.2:
                        results.append({
                            'query':   kw,
                            'traffic': f"Interest: {int(series.iloc[-1])}",
                            'source':  'pytrends IOT rising',
                        })
        logger.info(f"pytrends IOT: {len(results)} rising ingredient trends")
    except Exception as e:
        logger.warning(f"pytrends IOT failed: {e}")
    return results


# ──────────────────────────────────────────
# Method 5: News RSS fallback
# ──────────────────────────────────────────

def _fetch_news_rss() -> list:
    results = []
    for q in NEWS_RSS_QUERIES:
        try:
            url = f"https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"
            feed = feedparser.parse(url)
            for entry in feed.entries[:4]:
                title = getattr(entry, 'title', '').strip()
                if title and _is_beauty(title):
                    results.append({
                        'query':   title,
                        'traffic': 'News',
                        'source':  'Google News RSS',
                    })
        except Exception as e:
            logger.warning(f"News RSS failed for {q}: {e}")
    logger.info(f"News RSS fallback: {len(results)} entries")
    return results


# ──────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────

def get_google_trends() -> list:
    """
    Multi-method Google Trends collector.
    Returns list of dicts: {query, traffic, source}
    Starts with free RSS endpoints (no 429), falls back to pytrends.
    """
    seen: set = set()
    all_results: list = []

    def add(items: list):
        for item in items:
            q = item.get('query', '').strip()
            if q and q.lower() not in seen:
                seen.add(q.lower())
                all_results.append(item)

    # Method 1: Realtime RSS (beauty-filtered, all categories)
    add(_fetch_trends_rss(hours=48))
    time.sleep(random.uniform(1.5, 3.0))

    # Method 2: Realtime RSS with Beauty & Fashion category filter
    add(_fetch_trends_rss(hours=168, cat='44'))  # last 7 days, beauty cat
    time.sleep(random.uniform(1.5, 3.0))

    # Method 3: Additional RSS windows for deeper coverage
    add(_fetch_wider_rss())

    logger.info(f"Google Trends RSS total: {len(all_results)} before pytrends")

    # Methods 4+5: pytrends for breakout/rising queries — only if RSS gave < 5
    if len(all_results) < 5:
        logger.info("RSS gave < 5 beauty results — trying pytrends breakouts...")
        add(_fetch_pytrends_rising(BROAD_SEEDS, cat=44))
        if len(all_results) < 5:
            add(_fetch_pytrends_rising(INGREDIENT_SEEDS, cat=0))
        if len(all_results) < 5:
            add(_fetch_pytrends_iot(INGREDIENT_SEEDS))

    # Method 6: News RSS fallback if still sparse
    if len(all_results) < 5:
        logger.info("pytrends sparse — using News RSS fallback")
        add(_fetch_news_rss())

    # Always add news RSS for context richness (different signal type)
    if len(all_results) < 20:
        add(_fetch_news_rss())

    logger.info(f"Google Trends final: {len(all_results)} unique trends")
    return all_results


def get_breakout_trends(keywords: list) -> list:
    """Detect breakout keywords for specific beauty ingredients."""
    return _fetch_pytrends_rising(keywords[:5], cat=44)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = get_google_trends()
    print(f"\n{len(results)} Google trends:")
    for r in results[:15]:
        print(f"  [{r['source']}] {r['query']} — {r['traffic']}")
