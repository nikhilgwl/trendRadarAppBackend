import os
import json
import time
import logging
from google import genai
from google.genai import types
from dotenv import load_dotenv
from brain.product_matcher import match_products

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# User-specified model (Fix: updated to gemini-2.5-flash-lite-preview per product team)
_MODEL_ID = "gemini-2.5-flash-lite-preview-06-17"

# D: Indian subreddits carry higher signal weight for HUL India
_INDIAN_SUBREDDITS = {
    "indianskincareaddicts", "indianmakeupaddicts", "indianhaircare",
}

SYSTEM_INSTRUCTION = """You are an Expert Beauty Intelligence Analyst for HUL (Hindustan Unilever) India.
Transform raw multi-platform beauty signals into precise, actionable marketing intelligence.

Labels: [REDDIT EXCLUSIVE] | [GOOGLE/SOCIAL] | [NEWS/RSS] | [CROSS-PLATFORM] | [INGREDIENT BREAKOUT]
Categories: Skincare | Haircare | Makeup

SOURCE RELIABILITY HIERARCHY (E — weight signals accordingly):
1. Google Trends (search intent) — HIGHEST reliability, reflects actual consumer demand
2. Amazon / Nykaa Bestsellers (purchase intent) — VERY HIGH, reflects what Indians are actually buying
3. Reddit India threads (IndianSkincareAddicts, IndianMakeupAddicts, IndianHaircare) — HIGH, organic Indian consumer voice
4. Reddit Global threads — MEDIUM, may not reflect India market
5. Instagram posts — MODERATE, often includes paid/sponsored content; verify with other sources
6. X/Twitter trending — VARIES, broad signal, confirm with search data
7. News/RSS — CONTEXT only, use to understand narrative, not consumer intent

URGENCY CRITERIA (G — assign one per trend):
- URGENT: Trend appears on 3+ sources OR spikes on X/Instagram in last 24h → act within 48 hours
- MONITOR: Trend on 1-2 sources, building momentum → review in 2 weeks
- WATCH: Single weak signal, early-stage → include in quarterly planning

RULES:
1. trend_name: Hyper-specific (e.g. "Cloud Skin Matte Finish", not "Matte Makeup")
2. context: Explain WHY it's trending — cite specific platforms and source reliability (max 25 words)
3. result: One concrete HUL action — messaging pivot, content hook, or category to prioritize (max 20 words)
4. content_idea: Specific, bold activation HUL can execute in the next 2 weeks (max 30 words)
5. urgency: One of URGENT | MONITOR | WATCH based on criteria above
6. competitor_intel: Call out any competitor brand explicitly if detected in raw data, else null

OUTPUT: JSON array of 3 to 7 objects (H — output as many as the data genuinely supports, no padding):
[
  {
    "label": "[REDDIT EXCLUSIVE]",
    "category": "Skincare",
    "trend_name": "...",
    "source_platform": "...",
    "metric": "...",
    "context": "...",
    "result": "...",
    "content_idea": "...",
    "urgency": "URGENT",
    "competitor_intel": "Minimalist's Niacinamide 10% trending in 5 Reddit threads" or null
  }
]

IMPORTANT: Only include a trend if it has genuine signal strength. Output fewer, stronger trends rather than padding to hit a number."""


def get_categorized_trends(
    config,
    google_trends,
    reddit_posts,
    rss_headlines,
    social_trends=None,
    pinterest_trends=None,
    amazon_trends=None,
    twitter_trends=None,
    competitor_signals=None,
    nykaa_trends=None,
    instagram_trends=None,
) -> list:
    """
    Synthesise raw data into 3-7 intelligence trends using Gemini.
    Product matching done post-Gemini — zero extra LLM tokens.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not set.")
        return []

    # Retry with exponential backoff
    for attempt in range(3):
        try:
            result = _call_gemini(
                api_key, config, google_trends, reddit_posts, rss_headlines,
                social_trends, pinterest_trends, amazon_trends, twitter_trends,
                competitor_signals, nykaa_trends, instagram_trends,
            )
            if result:
                return result
        except Exception as e:
            wait = 2 ** attempt
            if attempt < 2:
                logger.warning(f"Gemini attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"Gemini failed after 3 attempts: {e}")
    return []


def _call_gemini(
    api_key, config, google_trends, reddit_posts, rss_headlines,
    social_trends, pinterest_trends, amazon_trends, twitter_trends,
    competitor_signals, nykaa_trends, instagram_trends,
) -> list:
    client = genai.Client(api_key=api_key)

    # D: label Indian vs global subreddits so Gemini weights them correctly
    reddit_text = "\n".join(
        f"- {p['title']} [{p.get('score', 0)} upvotes, "
        f"r/{p['subreddit']} {'🇮🇳 INDIA-SPECIFIC' if p.get('subreddit', '').lower() in _INDIAN_SUBREDDITS else 'GLOBAL'}]"
        for p in reddit_posts[:60]
    )

    news_text = "\n".join(f"- {r['title']}" for r in rss_headlines[:25])

    # E: Google Trends labeled with trust level
    google_text = "\n".join(
        f"- {g['query']} [{g.get('traffic', '')}] (search intent — high trust)"
        if isinstance(g, dict) else f"- {g}"
        for g in (google_trends or [])[:20]
    )

    # E: Amazon/Nykaa labeled as purchase intent
    amazon_text = "\n".join(
        f"- #{r.get('rank')} {r.get('product_name')} ({r.get('brand', '')}) [purchase data]"
        for r in (amazon_trends or [])[:15]
    )
    nykaa_text = "\n".join(
        f"- {p.get('product_name', p.get('title', ''))} [{p.get('category', '')}] [purchase data]"
        for p in (nykaa_trends or [])[:15]
    )

    twitter_text = "\n".join(
        f"- {t[:120]}" if isinstance(t, str) else f"- {t.get('query', str(t))[:120]}"
        for t in (twitter_trends or [])[:15]
    )
    pinterest_text = "\n".join(
        f"- {p['title'] if isinstance(p, dict) else str(p)}"
        for p in (pinterest_trends or [])[:12]
    )

    # E: Instagram labeled as moderate trust (may include sponsored)
    instagram_text = "\n".join(
        f"- [{p.get('likes', 0)} likes] {p.get('caption', '')[:150]} [may be sponsored]"
        for p in (instagram_trends or [])[:20]
        if p.get('caption', '').strip()
    )

    comp_text = ""
    if competitor_signals:
        lines = [
            f"- [{s.get('platform')}] {s.get('competitor')}: \"{s.get('mention_text', '')[:100]}\""
            for s in competitor_signals[:20]
        ]
        comp_text = "\n".join(lines)

    prompt = f"""REDDIT DISCUSSIONS (🇮🇳 = Indian community, weighted higher):
{reddit_text}

GOOGLE TRENDS INDIA (search intent — highest trust):
{google_text}

X/TWITTER TRENDING INDIA:
{twitter_text}

PINTEREST:
{pinterest_text}

NEWS/RSS (context only):
{news_text}

AMAZON INDIA BESTSELLERS (purchase intent — very high trust):
{amazon_text}

NYKAA INDIA BESTSELLERS (purchase intent — very high trust):
{nykaa_text}

INSTAGRAM BEAUTY POSTS (moderate trust — may include sponsored):
{instagram_text if instagram_text else "No Instagram data."}

COMPETITOR SIGNALS (pre-detected — use for competitor_intel field):
{comp_text if comp_text else "No competitor signals detected in this batch."}"""

    gemini_cfg = types.GenerateContentConfig(
        system_instruction=SYSTEM_INSTRUCTION,
        temperature=0.1,
        response_mime_type="application/json",
    )

    logger.info(f"Calling Gemini ({_MODEL_ID}) for trend synthesis...")
    response = client.models.generate_content(
        model=_MODEL_ID,
        contents=prompt,
        config=gemini_cfg,
    )

    if not response.text:
        return []

    trends = json.loads(response.text.strip())

    # Post-process: attach product matches (pure keyword code, no Gemini)
    for trend in trends:
        search_text = f"{trend.get('trend_name', '')} {trend.get('context', '')} {trend.get('category', '')}"
        trend["hul_products"] = match_products(search_text, limit=5, category_hint=trend.get('category', ''))

    logger.info(f"Gemini returned {len(trends)} trends.")
    return trends
