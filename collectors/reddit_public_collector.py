import json
import time
import random
import logging
import requests
import feedparser
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def _fetch_via_rss(clean_name, now):
    """
    Fetch posts via Reddit's public RSS feed.
    RSS feeds are served from a different pipeline and not blocked on cloud IPs.
    """
    posts = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36'
        }
        url = f"https://www.reddit.com/r/{clean_name}/hot.rss?limit=30"
        r = requests.get(url, headers=headers, timeout=15)

        if r.status_code != 200:
            logger.warning(f"RSS feed returned {r.status_code} for r/{clean_name}")
            return posts

        feed = feedparser.parse(r.content)
        for entry in feed.entries:
            title = entry.get('title', '').encode('utf-8', errors='ignore').decode('utf-8')
            link = entry.get('link', '')
            published = entry.get('published_parsed')

            if published:
                created_at = datetime.fromtimestamp(time.mktime(published), timezone.utc)
                age_hours = (now - created_at).total_seconds() / 3600
            else:
                age_hours = 12  # Assume recent if no timestamp

            if 0 < age_hours <= 24:
                # RSS doesn't give upvotes; use comment count as proxy
                summary = entry.get('summary', '')
                posts.append({
                    "title": title,
                    "score": 0,  # RSS doesn't expose scores
                    "num_comments": 0,
                    "subreddit": clean_name,
                    "velocity": 1.0 / max(age_hours, 0.1),
                    "url": link
                })

        logger.info(f"✅ RSS: {len(posts)} posts from r/{clean_name}")
    except Exception as e:
        logger.warning(f"RSS fetch failed for r/{clean_name}: {e}")

    return posts


def _fetch_via_playwright(clean_name, now):
    """
    Fetch posts via Playwright headless browser (works locally, blocked on cloud IPs).
    Falls back gracefully if Playwright is not available.
    """
    posts = []
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return posts

    endpoints = ['hot', 'top?t=day']
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 800},
                locale='en-US',
                timezone_id='Asia/Kolkata',
            )
            page = context.new_page()
            page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,ttf}", lambda route: route.abort())

            for endpoint in endpoints:
                try:
                    url = f"https://www.reddit.com/r/{clean_name}/{endpoint}.json?limit=30"
                    response = page.goto(url, timeout=20000, wait_until='domcontentloaded')
                    
                    if not response or not response.ok:
                        logger.warning(f"Playwright: Failed to load {endpoint} for r/{clean_name} (Status: {response.status if response else 'No Response'})")
                        continue

                    # Check if response is actually JSON
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' not in content_type:
                        logger.debug(f"Playwright: Non-JSON response for {endpoint} in r/{clean_name}")
                        continue

                    raw_text = page.inner_text('body')
                    if not raw_text.strip().startswith('{'):
                        continue

                    data = json.loads(raw_text)
                    children = data.get('data', {}).get('children', [])
                    logger.info(f"✅ Playwright: {len(children)} posts from r/{clean_name} ({endpoint})")

                    for post_data in children:
                        post = post_data.get('data', {})
                        created_utc = post.get('created_utc')
                        if not created_utc:
                            continue
                        created_at = datetime.fromtimestamp(created_utc, timezone.utc)
                        age_hours = (now - created_at).total_seconds() / 3600
                        if 0 < age_hours <= 24:
                            score = post.get('score', 0)
                            title = post.get('title', '').encode('utf-8', errors='ignore').decode('utf-8')
                            posts.append({
                                "title": title,
                                "score": score,
                                "num_comments": post.get('num_comments', 0),
                                "subreddit": clean_name,
                                "velocity": score / max(age_hours, 0.1),
                                "url": f"https://reddit.com{post.get('permalink')}"
                            })
                except Exception as e:
                    logger.warning(f"Playwright failed for r/{clean_name} ({endpoint}): {e}")
                time.sleep(random.uniform(1.0, 2.0))

            browser.close()
    except Exception as e:
        logger.warning(f"Playwright browser error: {e}")

    return posts


def get_reddit_trends(config):
    """
    Context-Aware Reddit collector:
    Broadly collects beauty discussions and uses 'Pattern Recognition' 
    to prioritize trends rather than strict keyword matching.
    """
    subreddits = [s for c in config['categories'] for s in c['subreddits']]
    
    # Pattern keywords for discovery (not just specific ingredients)
    discovery_patterns = [
        "launch", "viral", "spotted", "new", "review", "comparison", 
        "dupe", "alternative", "holy grail", "routine", "method",
        "ingredients", "worth it", "hype", "spotted", "launching"
    ]
    
    all_posts = []
    now = datetime.now(timezone.utc)
    
    for sub_name in subreddits:
        clean_name = sub_name.replace("r/", "") if sub_name.startswith("r/") else sub_name
        playwright_posts = _fetch_via_playwright(clean_name, now)
        all_posts.extend(playwright_posts)
        rss_posts = _fetch_via_rss(clean_name, now)
        all_posts.extend(rss_posts)
        time.sleep(random.uniform(0.5, 1.5))

    # Deduplicate by URL
    unique_posts = {p['url']: p for p in all_posts}.values()

    # Contextual Filtering: Keep posts that look like "Trends" or "Intelligence"
    # rather than just personal medical questions.
    filtered_posts = []
    for post in unique_posts:
        title_lower = post['title'].lower()
        
        # Pattern 1: High Engagement (Automatically a trend signal)
        if post.get('score', 0) > 50 or post.get('num_comments', 0) > 20:
            filtered_posts.append(post)
            continue
            
        # Pattern 2: Discovery Keywords (Looking for newness)
        if any(p in title_lower for p in discovery_patterns):
            filtered_posts.append(post)
            continue
            
        # Pattern 3: Ingredient-like terminology (Discovery of new actives)
        # Regex-like check for common beauty suffixes
        if any(suffix in title_lower for suffix in [" acid", "amide", "inol", " peptide", " serum", " cream"]):
            filtered_posts.append(post)
            continue
    
    # Sort by score/engagement
    results = sorted(filtered_posts, key=lambda x: x['score'], reverse=True)
    logger.info(f"TOTAL PATTERN-RELEVANT POSTS: {len(results)} (Broadened from {len(unique_posts)})")
    return results
