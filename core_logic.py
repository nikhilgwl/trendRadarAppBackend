import asyncio
import yaml
import logging
import json
import os
import re
from datetime import datetime, timedelta

from collectors.google_trends import get_google_trends
from collectors.reddit_public_collector import get_reddit_trends
from collectors.rss_collector import get_rss_trends
from collectors.social_collector import get_social_trends
from collectors.pinterest_collector import get_pinterest_trends
from collectors.amazon_collector import get_amazon_trends
from collectors.twitter_collector import get_twitter_trends
from collectors.instagram_collector import get_instagram_trends
from collectors.nykaa_collector import get_nykaa_trends
from brain.gemini_filter import get_categorized_trends
import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("run_log.txt"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

RAW_DATA_FILE  = "raw_trends.json"
AI_SUMMARY_FILE = "daily_beauty_insights.json"


def _load_config() -> dict:
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"config.yaml load failed: {e}")
        return {}


def _scan_all_brand_signals(config: dict, raw_payload: dict) -> list[dict]:
    """
    Scan all collected text for both competitor mentions AND HUL own-brand mentions.
    Competitor signals saved as-is. HUL brand signals prefixed with 'HUL:' in the
    competitor field so they can be distinguished without schema changes.
    """
    competitors = config.get("competitors", [])
    own_brands  = config.get("brand_portfolio", [])

    if not competitors and not own_brands:
        return []

    # Build case-insensitive lookup: lowercase match → canonical name from config
    comp_canon = {c.lower(): c for c in competitors}
    brand_canon = {b.lower(): b for b in own_brands}

    # Two patterns: one for competitors, one for HUL's own brands
    # Sort longest-first so "The Derma Co" matches before "Co"
    comp_pattern = re.compile(
        r'\b(' + '|'.join(sorted([re.escape(c) for c in competitors], key=len, reverse=True)) + r')\b',
        re.IGNORECASE
    ) if competitors else None

    brand_pattern = re.compile(
        r'\b(' + '|'.join(sorted([re.escape(b) for b in own_brands], key=len, reverse=True)) + r')\b',
        re.IGNORECASE
    ) if own_brands else None

    now = datetime.now().isoformat()

    # Sources to scan: (key_in_payload, platform_label, text_extractor, url_extractor)
    sources = [
        ("reddit",    "Reddit",           lambda x: x.get("title", ""),                         lambda x: x.get("url")),
        ("rss",       "News/RSS",          lambda x: x.get("title", "") + " " + x.get("summary", ""), lambda x: x.get("link")),
        ("social",    "Social/Twitter",   lambda x: x if isinstance(x, str) else str(x),        lambda x: None),
        ("twitter",   "Twitter",          lambda x: x if isinstance(x, str) else str(x),        lambda x: None),
        ("amazon",    "Amazon Bestseller",lambda x: x.get("product_name", ""),                  lambda x: None),
        ("nykaa",     "Nykaa Bestseller", lambda x: x.get("product_name", "") or x.get("title", ""), lambda x: None),
        ("instagram", "Instagram",        lambda x: x.get("caption", ""),                       lambda x: x.get("url")),
    ]

    signals = []
    # Use a set to prevent same brand matching multiple times in SAME item text
    run_seen = set()

    for key, platform, get_text, get_url in sources:
        for item in raw_payload.get(key, []):
            text = get_text(item)
            url  = get_url(item)
            if not text:
                continue

            # Competitor mentions
            if comp_pattern:
                matched_in_this_item = set()
                for m in comp_pattern.finditer(text):
                    canonical = comp_canon.get(m.group(0).lower(), m.group(0))
                    
                    # Context validation for common words (like 'Simple')
                    if canonical.lower() == "simple":
                        # Require specific product anchors for the 'Simple' brand
                        anchors = ["facewash", "face wash", "cleanser", "moisturizer", "serum", "spf", "sunscreen", "kind to skin", "sensitive", "micellar", "toner", "skincare", "skin care"]
                        # Negative anchors to exclude generic usage
                        exclude = ["routine", "steps", "tips", "ways", "guide", "method", "process", "hack", "trick"]
                        
                        lower_text = text.lower()
                        has_anchor = any(a in lower_text for a in anchors)
                        has_exclude = any(e in lower_text for e in exclude)
                        
                        if not has_anchor or has_exclude:
                            continue

                    if canonical.lower() == "wow":
                        # Require beauty context for 'Wow'
                        beauty_context = ["skin", "face", "hair", "beauty", "care", "wash", "serum", "cream", "gel", "oil", "spf", "sunscreen", "makeup", "look"]
                        if not any(kw in text.lower() for kw in beauty_context):
                            continue

                    if canonical not in matched_in_this_item:
                        matched_in_this_item.add(canonical)
                        signals.append({
                            "competitor":   canonical,
                            "platform":     platform,
                            "mention_text": text[:300],
                            "source_url":   url,
                            "collected_at": now,
                        })

            # HUL own-brand mentions
            if brand_pattern:
                matched_in_this_item = set()
                for m in brand_pattern.finditer(text):
                    canonical = brand_canon.get(m.group(0).lower(), m.group(0))
                    
                    # Context validation for 'Simple' (HUL brand)
                    if canonical.lower() == "simple":
                        anchors = ["facewash", "face wash", "cleanser", "moisturizer", "serum", "spf", "sunscreen", "kind to skin", "sensitive", "micellar", "toner", "skincare", "skin care"]
                        exclude = ["routine", "steps", "tips", "ways", "guide", "method", "process", "hack", "trick"]
                        
                        lower_text = text.lower()
                        has_anchor = any(a in lower_text for a in anchors)
                        has_exclude = any(e in lower_text for e in exclude)
                        
                        if not has_anchor or has_exclude:
                            continue

                    if canonical not in matched_in_this_item:
                        matched_in_this_item.add(canonical)
                        signals.append({
                            "competitor":   f"HUL:{canonical}",
                            "platform":     platform,
                            "mention_text": text[:300],
                            "source_url":   url,
                            "collected_at": now,
                        })

    # Deduplicate within this run AND against what's already in DB (last 24h)
    # Deduplication key includes source_url to distinguish same text from different sources
    seen = set()
    try:
        existing = db.load_competitor_signals(days=1)
        for sig in existing:
            # Key: (brand, snippet, url_or_platform)
            seen.add((sig["competitor"].lower(), sig["mention_text"][:60], sig.get("source_url") or sig["platform"]))
    except Exception:
        pass

    unique = []
    for s in signals:
        key_tuple = (s["competitor"].lower(), s["mention_text"][:60], s.get("source_url") or s["platform"])
        if key_tuple not in seen:
            seen.add(key_tuple)
            unique.append(s)

    comp_count  = sum(1 for s in unique if not s["competitor"].startswith("HUL:"))
    brand_count = sum(1 for s in unique if s["competitor"].startswith("HUL:"))
    logger.info(f"Brand signals: {comp_count} competitor + {brand_count} own-brand mentions")
    return unique


def _enrich_with_history(trends: list) -> list:
    """
    I: For each trend, search 30-day historical DB data to find first_seen date
    and calculate days_tracking. Adds is_new flag for brand-new breakouts.
    """
    today = datetime.now().date()
    for trend in trends:
        trend_name = trend.get("trend_name", "")
        if not trend_name:
            trend.update({"first_seen": today.isoformat(), "days_tracking": 0, "is_new": True})
            continue

        # Use 2 most meaningful words from trend name for history search
        words = [w for w in trend_name.lower().split() if len(w) > 3]
        earliest = today

        for word in words[:2]:
            try:
                results = db.search_raw_trends(word, days=30)
                for r in results:
                    raw_dt = r.get("collected_at", "")
                    if not raw_dt:
                        continue
                    try:
                        dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).date()
                        if dt < earliest:
                            earliest = dt
                    except Exception:
                        pass
            except Exception:
                pass

        days = (today - earliest).days
        trend["first_seen"]    = earliest.isoformat()
        trend["days_tracking"] = days
        trend["is_new"]        = days <= 1

    return trends


async def collect_raw_data() -> dict | None:
    logger.info("Starting raw data collection (all platforms)...")
    config = _load_config()
    if not config:
        return None

    rss_urls = [r for c in config.get("categories", []) for r in c.get("rss", [])]

    # ── IP Protection ────────────────────────────────────────────────────────
    # Amazon and Nykaa scrape real e-commerce pages; too-frequent requests risk
    # IP bans. We collect them at most once per calendar day: if today's data
    # is already in Supabase, skip the HTTP scrape and load from DB instead.
    amazon_cached = db.has_platform_data_for_today("amazon")
    nykaa_cached  = db.has_platform_data_for_today("nykaa")
    if amazon_cached:
        logger.info("🛡️  Amazon: data already collected today — using DB cache (IP protection)")
    if nykaa_cached:
        logger.info("🛡️  Nykaa: data already collected today — using DB cache (IP protection)")

    def _skip() -> list:
        return []

    results = await asyncio.gather(
        asyncio.to_thread(get_google_trends),                                                            # 0
        asyncio.to_thread(get_reddit_trends, config),                                                    # 1
        asyncio.to_thread(get_rss_trends, rss_urls),                                                    # 2
        asyncio.to_thread(get_social_trends, config),                                                    # 3
        asyncio.to_thread(get_pinterest_trends, config),                                                 # 4
        asyncio.to_thread(_skip) if amazon_cached else asyncio.to_thread(get_amazon_trends, config),    # 5
        asyncio.to_thread(get_twitter_trends, config),                                                   # 6
        asyncio.to_thread(get_instagram_trends, config),                                                 # 7
        asyncio.to_thread(_skip) if nykaa_cached  else asyncio.to_thread(get_nykaa_trends, config),     # 8
        return_exceptions=True,
    )

    def safe(val, default):
        if isinstance(val, Exception):
            logger.warning(f"Collector error: {val}")
            return default
        return val or default

    google    = safe(results[0], [])
    reddit    = safe(results[1], [])
    rss       = safe(results[2], [])
    social    = safe(results[3], [])
    pinterest = safe(results[4], [])
    # Marketplace platforms: use fresh result OR cached DB row
    amazon    = db.load_platform_data_for_today("amazon") if amazon_cached else safe(results[5], [])
    twitter   = safe(results[6], [])
    instagram = safe(results[7], [])
    nykaa     = db.load_platform_data_for_today("nykaa")  if nykaa_cached  else safe(results[8], [])

    raw_payload = {
        "timestamp": datetime.now().isoformat(),
        "google":    google,
        "reddit":    reddit,
        "rss":       rss,
        "social":    social,
        "pinterest": pinterest,
        "amazon":    amazon,
        "twitter":   twitter,
        "instagram": instagram,
        "nykaa":     nykaa,
    }

    with open(RAW_DATA_FILE, "w") as f:
        json.dump(raw_payload, f, indent=2)

    # Only persist platforms that were freshly scraped this run.
    # Skipping cached marketplace platforms avoids a needless delete+reinsert cycle.
    cached_platforms = set()
    if amazon_cached:
        cached_platforms.add("amazon")
    if nykaa_cached:
        cached_platforms.add("nykaa")

    if cached_platforms:
        save_payload = {k: v for k, v in raw_payload.items() if k not in cached_platforms}
        db.save_raw_trends(save_payload)
    else:
        db.save_raw_trends(raw_payload)

    # B: scan for both competitor + HUL own-brand mentions
    # (uses full raw_payload including cached marketplace data — dedup handles repeats)
    all_signals = _scan_all_brand_signals(config, raw_payload)
    if all_signals:
        db.save_competitor_signals(all_signals)

    logger.info(
        f"Collection complete: google={len(google)}, reddit={len(reddit)}, "
        f"rss={len(rss)}, social={len(social)}, pinterest={len(pinterest)}, "
        f"amazon={len(amazon)} {'(cached)' if amazon_cached else '(fresh)'}, "
        f"twitter={len(twitter)}, instagram={len(instagram)}, "
        f"nykaa={len(nykaa)} {'(cached)' if nykaa_cached else '(fresh)'}"
    )
    return raw_payload


async def generate_ai_summary() -> dict | None:
    logger.info("Generating AI summary...")
    config = _load_config()

    raw_payload = db.load_raw_trends()
    if not raw_payload and os.path.exists(RAW_DATA_FILE):
        with open(RAW_DATA_FILE, "r") as f:
            raw_payload = json.load(f)

    if not raw_payload:
        logger.warning("No raw data found. Collecting first...")
        raw_payload = await collect_raw_data()

    if not raw_payload:
        return None

    # Load only competitor (non-HUL) signals for Gemini context
    all_signals = db.load_competitor_signals(days=7)
    competitor_signals = [s for s in all_signals if not s["competitor"].startswith("HUL:")]

    trends = get_categorized_trends(
        config,
        google_trends=raw_payload.get("google", []),
        reddit_posts=raw_payload.get("reddit", []),
        rss_headlines=raw_payload.get("rss", []),
        social_trends=raw_payload.get("social", []),
        pinterest_trends=raw_payload.get("pinterest", []),
        amazon_trends=raw_payload.get("amazon", []),
        twitter_trends=raw_payload.get("twitter", []),
        competitor_signals=competitor_signals,
        nykaa_trends=raw_payload.get("nykaa", []),
        instagram_trends=raw_payload.get("instagram", []),
    )

    if not trends:
        return None

    # C: flag white-space gap opportunities (trend with no matching HUL product)
    for trend in trends:
        trend["gap_opportunity"] = len(trend.get("hul_products", [])) == 0

    # I: enrich each trend with historical first_seen + days_tracking
    trends = _enrich_with_history(trends)

    ai_payload = {"timestamp": datetime.now().isoformat(), "trends": trends}

    with open(AI_SUMMARY_FILE, "w") as f:
        json.dump(ai_payload, f, indent=2)
    db.save_ai_digest(ai_payload)

    logger.info(f"AI summary saved: {len(trends)} trends.")
    return ai_payload


async def run_pipeline():
    await collect_raw_data()


if __name__ == "__main__":
    asyncio.run(run_pipeline())
