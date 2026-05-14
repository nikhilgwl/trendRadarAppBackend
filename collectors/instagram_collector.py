"""
Instagram beauty trend collector using instagrapi (authenticated).
Credentials: INSTAGRAM_USERNAME and INSTAGRAM_PASSWORD in .env
Session is cached to instagram_session.json to avoid repeated logins.

Rate limits: ~200 req/hour. We stay well below that (max 50 req/run).
"""
import os
import json
import logging
import time
import random

logger = logging.getLogger(__name__)

SESSION_FILE = os.path.join(os.path.dirname(__file__), '..', 'instagram_session.json')

BEAUTY_HASHTAGS = [
    "skincareindia",
    "haircareindia",
    "makeupindia",
    "indianskincare",
    "beautytipsindia",
    "skincareroutine",
    "hairfall",
    "glowingskin",
    "sunscreenindia",
    "niacinamide",
    "beautyhacksindia",
    "skincareproductsindia",
    "makeupartistindia",
    "indianskincaretips",
    "desibeauty",
    "indianmakeup",
    "skincarejunkieindia",
    "ayurvedicskincare",
    "glassskinindia",
    "kbeautyindia",
]


def _get_client():
    """Return an authenticated instagrapi client, reusing cached session."""
    try:
        from instagrapi import Client
    except ImportError:
        logger.error("instagrapi not installed. Run: pip install instagrapi")
        return None

    username = os.getenv("INSTAGRAM_USERNAME", "").strip()
    password = os.getenv("INSTAGRAM_PASSWORD", "").strip()

    if not username or not password:
        logger.warning("INSTAGRAM_USERNAME / INSTAGRAM_PASSWORD not set in .env — Instagram collector disabled.")
        return None

    cl = Client()
    cl.delay_range = [2, 5]  # polite random delay between requests

    # Try loading cached session
    if os.path.exists(SESSION_FILE):
        try:
            cl.load_settings(SESSION_FILE)
            cl.login(username, password)
            logger.info("Instagram: reused cached session")
            return cl
        except Exception as e:
            logger.warning(f"Instagram session load failed ({e}), re-logging in...")

    # Fresh login
    try:
        cl.login(username, password)
        cl.dump_settings(SESSION_FILE)
        logger.info("Instagram: fresh login successful, session cached")
        return cl
    except Exception as e:
        logger.error(f"Instagram login failed: {e}")
        return None


def get_instagram_trends(config=None) -> list[dict]:
    """
    Fetch top posts from beauty hashtags on Instagram.
    Returns list of {hashtag, caption, likes, comments, url}
    """
    cl = _get_client()
    if not cl:
        return []

    results = []
    tags_to_fetch = random.sample(BEAUTY_HASHTAGS, min(8, len(BEAUTY_HASHTAGS)))

    for tag in tags_to_fetch:
        try:
            medias = cl.hashtag_medias_top(tag, amount=15)
            for m in medias:
                caption = (m.caption_text or "")[:500].replace("\n", " ")
                results.append({
                    "hashtag":  f"#{tag}",
                    "caption":  caption,
                    "likes":    m.like_count,
                    "comments": m.comment_count,
                    "url":      f"https://www.instagram.com/p/{m.code}/",
                })
            logger.info(f"Instagram #{tag}: {len(medias)} posts")
            time.sleep(random.uniform(3.0, 6.0))  # stay within rate limits
        except Exception as e:
            logger.warning(f"Instagram #{tag} failed: {e}")

    logger.info(f"Instagram collector total: {len(results)} posts")
    return results
