"""
Twitter/X beauty trend collector.
Primary: X API v1.1 trends/place.json (India) using guest token via curl_cffi.
Fallback: trends24.in and getdaytrends.com scraping.
No login or API keys required.
"""
import logging
import random
import re
import time
import feedparser
from curl_cffi import requests as cf_requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BEAUTY_FILTER = [
    'skin', 'hair', 'makeup', 'beauty', 'serum', 'cream', 'glow', 'lip',
    'skincare', 'haircare', 'sunscreen', 'moisturizer', 'shampoo', 'conditioner',
    'retinol', 'niacinamide', 'vitamin', 'toner', 'face', 'foundation',
    'kajal', 'kohl', 'blush', 'nail', 'scalp', 'dandruff', 'hairfall',
    'lakme', 'dove', 'pond', 'tresemme', 'indulekha', 'mamaearth',
    'minimalist', 'plum', 'derma', 'spf', 'acne', 'pore', 'glowing',
    'brightening', 'fragrance', 'routine', 'ingredient', 'sunblock', 'hydrat',
    'oilfree', 'viral', 'trend', 'hack', 'review', 'haul', 'glass skin',
    'k-beauty', 'double cleanse', 'slugging', 'skin barrier', 'hair care',
    'skin care', 'doctor', 'dermatologist',
]

EXCLUDE_FILTER = [
    'cricket', 'ipl', 'match', 'score', 'team', 'player', 'election', 'vote',
    'minister', 'politics', 'stadium', 'ball', 'wicket', 't20', 'odi', 'test',
    'world cup', 'stock', 'share price', 'nifty', 'sensex', 'market', 'gold price',
    'weather', 'rain', 'cyclone', 'earthquake', 'accident', 'death', 'killed',
    'hardik pandya', 'sunil gavaskar', 'virat kohli', 'dhoni', 'rohit sharma',
    'news', 'live', 'today', 'movie', 'actor', 'actress', 'trailer', 'song',
    'modi', 'rahul gandhi', 'bjp', 'congress', 'government', 'police', 'court',
]

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'en-IN,en;q=0.9',
}

# India WOEID + major city WOEIDs for broader coverage
_INDIA_WOEIDS = [
    23424848,  # India
    2295411,   # Mumbai
    20070458,  # Delhi
    1277333,   # Bangalore
]

_bearer_cache = {'token': None}


def _is_beauty(text: str) -> bool:
    t = text.lower()
    if any(ex in t for ex in EXCLUDE_FILTER):
        return False
    return any(kw in t for kw in BEAUTY_FILTER)


def _get_bearer_token() -> str:
    """Fetch current bearer token from X.com JS bundle. Cached per process."""
    if _bearer_cache['token']:
        return _bearer_cache['token']
    try:
        session = cf_requests.Session(impersonate='chrome124')
        r = session.get('https://x.com', timeout=20)
        js_urls = re.findall(
            r'src="(https://abs\.twimg\.com/responsive-web/client-web/main\.[^"]+\.js)"', r.text)
        if js_urls:
            js_r = session.get(js_urls[0], timeout=30)
            match = re.search(r'(AAAAAAAAAAAA[A-Za-z0-9%]+)', js_r.text)
            if match:
                _bearer_cache['token'] = match.group(1)
                logger.info('Bearer token refreshed from X.com JS bundle')
                return _bearer_cache['token']
    except Exception as e:
        logger.warning(f'Bearer token fetch failed: {e}')
    # Known-good fallback (may expire — refreshed above when possible)
    return 'AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'


def _fetch_x_api_trends() -> list:
    """
    Fetch India trending topics directly from X API v1.1 using a guest token.
    No login or API keys needed — curl_cffi bypasses Cloudflare TLS checks.
    Returns beauty-filtered trending topic names.
    """
    results = []
    try:
        bearer = _get_bearer_token()
        session = cf_requests.Session(impersonate='chrome124')
        session.get('https://x.com', timeout=20)

        # Get guest token
        rg = session.post(
            'https://api.twitter.com/1.1/guest/activate.json',
            headers={'Authorization': f'Bearer {bearer}',
                     'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=15,
        )
        if rg.status_code != 200:
            logger.warning(f'Guest token failed: {rg.status_code}')
            return []
        guest_token = rg.json().get('guest_token', '')
        if not guest_token:
            return []

        api_headers = {
            'Authorization': f'Bearer {bearer}',
            'x-guest-token': guest_token,
        }

        seen = set()
        for woeid in _INDIA_WOEIDS:
            try:
                r = session.get(
                    f'https://api.twitter.com/1.1/trends/place.json?id={woeid}',
                    headers=api_headers,
                    timeout=15,
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                trends = data[0].get('trends', [])
                for t in trends:
                    name = t.get('name', '').strip()
                    key = name.lower()
                    if name and key not in seen and _is_beauty(name):
                        seen.add(key)
                        results.append(name)
                logger.info(f'X API WOEID {woeid}: {len(trends)} trends total')
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                logger.warning(f'X API WOEID {woeid} failed: {e}')

        logger.info(f'X API guest trends: {len(results)} beauty trends')
    except Exception as e:
        logger.error(f'X API guest token flow failed: {e}')
    return results


def _fetch_trends24() -> list:
    """Scrape trends24.in for India X trending topics."""
    try:
        resp = cf_requests.get('https://trends24.in/india/', headers=_HEADERS,
                               impersonate='chrome', timeout=15)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        seen = set()
        items = []
        for section in soup.select('ol.trend-card__list')[:4]:
            for el in section.select('li a'):
                t = el.text.strip()
                key = t.lower()
                if t and key not in seen:
                    seen.add(key)
                    if _is_beauty(t):
                        items.append(t)
        logger.info(f'trends24.in: {len(items)} beauty trends')
        return items
    except Exception as e:
        logger.error(f'trends24.in failed: {e}')
        return []


def _fetch_getdaytrends() -> list:
    """Scrape getdaytrends.com for India X trending topics."""
    try:
        resp = cf_requests.get('https://getdaytrends.com/india/', headers=_HEADERS,
                               impersonate='chrome', timeout=15)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        items = [el.text.strip() for el in soup.select('td.main a') if el.text.strip()]
        beauty = [t for t in items if _is_beauty(t)]
        logger.info(f'getdaytrends: {len(beauty)} beauty trends')
        return beauty
    except Exception as e:
        logger.error(f'getdaytrends failed: {e}')
        return []


def get_twitter_trends(config=None) -> list:
    """
    Return beauty-relevant X trend signals for India.
    Primary: X API guest token (direct, real-time).
    Fallback: trends24.in, getdaytrends.com.
    """
    results = []
    seen = set()

    def add_unique(items):
        for t in items:
            key = t.lower()[:60]
            if key not in seen:
                seen.add(key)
                results.append(t)

    # Primary: X API guest token
    add_unique(_fetch_x_api_trends())

    # Fallback if API gave nothing
    if len(results) < 5:
        time.sleep(random.uniform(1, 2))
        add_unique(_fetch_trends24())

    if len(results) < 5:
        add_unique(_fetch_getdaytrends())

    logger.info(f'X/Twitter collector final: {len(results)} trends')
    return results


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    res = get_twitter_trends()
    for r in res:
        print(f'X Trend: {r}')
