import praw
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

def get_reddit_trends(subreddits):
    """
    Fetch top posts from subreddits and sort by upvote velocity.
    """
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT", "TrendRadarBot/1.0")
    )
    
    all_posts = []
    now = datetime.now(timezone.utc)
    
    for sub_name in subreddits:
        try:
            subreddit = reddit.subreddit(sub_name)
            for post in subreddit.hot(limit=15):
                created_at = datetime.fromtimestamp(post.created_utc, timezone.utc)
                age_hours = (now - created_at).total_seconds() / 3600
                
                # Filter by created_utc (last 6 hours)
                if 0 < age_hours <= 6:
                    velocity = post.score / max(age_hours, 0.1) # Upvotes per hour
                    all_posts.append({
                        "title": post.title,
                        "score": post.score,
                        "subreddit": sub_name,
                        "velocity": velocity,
                        "url": f"https://reddit.com{post.permalink}"
                    })
        except Exception as e:
            print(f"Reddit fetch failed for {sub_name}: {e}")
            
    # Sort by upvote velocity
    sorted_posts = sorted(all_posts, key=lambda x: x['velocity'], reverse=True)
    return sorted_posts

if __name__ == "__main__":
    # Test with a few subreddits
    test_subs = ["india", "bollywood"]
    trends = get_reddit_trends(test_subs)
    for t in trends[:5]:
        print(f"[{t['subreddit']}] {t['title']} - Velocity: {t['velocity']:.2f}")
