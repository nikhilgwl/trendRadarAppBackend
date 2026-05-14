import os
import json
import logging
from datetime import datetime, timedelta
from supabase import create_client, Client

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

_client: Client | None = None


def get_client() -> Client | None:
    global _client
    if _client:
        return _client
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase credentials missing — falling back to local JSON files.")
        return None
    try:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
        return _client
    except Exception as e:
        logger.error(f"Supabase init failed: {e}")
        return None


# ─────────────────────────────────────────────
# Raw trends  (historical, 6-month window)
# ─────────────────────────────────────────────

def save_raw_trends(payload: dict) -> bool:
    client = get_client()
    if not client:
        return False
    try:
        today = datetime.now().date().isoformat()
        platforms = [p for p in payload.keys() if p != "timestamp"]

        # Fix 6: delete today's existing rows for these platforms before inserting
        # — prevents duplicate rows when the pipeline runs more than once per day
        client.table("raw_trends").delete()\
            .eq("collected_date", today)\
            .in_("platform", platforms)\
            .execute()

        rows = [
            {
                "platform": platform,
                "data": payload[platform],
                "collected_at": payload.get("timestamp"),
                "collected_date": today,
            }
            for platform in platforms
        ]
        client.table("raw_trends").insert(rows).execute()

        # Rolling 6-month window: delete anything older than 180 days
        cutoff = (datetime.now() - timedelta(days=180)).date().isoformat()
        client.table("raw_trends").delete().lt("collected_date", cutoff).execute()

        logger.info(f"Saved {len(rows)} platform snapshots for {today}. Purged rows before {cutoff}.")
        return True
    except Exception as e:
        logger.error(f"save_raw_trends failed: {e}")
        return False


def load_raw_trends() -> dict | None:
    """Return the latest snapshot per platform."""
    client = get_client()
    if not client:
        return None
    try:
        # Fix 5: raise limit from 50 → 200 so all 9 platforms are always covered
        # even when the pipeline has run multiple times today
        resp = (
            client.table("raw_trends")
            .select("platform, data, collected_at")
            .order("collected_at", desc=True)
            .limit(200)
            .execute()
        )
        if not resp.data:
            return None

        result: dict = {}
        for row in resp.data:
            p = row["platform"]
            if p not in result:
                result[p] = row["data"]
                if "timestamp" not in result:
                    result["timestamp"] = row["collected_at"]
        return result or None
    except Exception as e:
        logger.error(f"load_raw_trends failed: {e}")
        return None


def search_raw_trends(query: str, days: int = 180) -> list:
    """
    Full-text search across historical raw_trends data.
    Fetches recent rows and filters in Python because Supabase PostgREST
    doesn't support ilike on JSONB cast columns (data::text).
    """
    client = get_client()
    if not client:
        return []
    try:
        cutoff = (datetime.now() - timedelta(days=days)).date().isoformat()

        resp = (
            client.table("raw_trends")
            .select("platform, data, collected_at, collected_date")
            .gte("collected_date", cutoff)
            .order("collected_at", desc=True)
            .limit(500)
            .execute()
        )

        q = query.lower()
        results = []
        seen_keys: set = set()

        for row in (resp.data or []):
            data = row.get("data") or []
            platform = row["platform"]
            collected_at = row["collected_at"]

            items = data if isinstance(data, list) else [data]
            for item in items:
                item_str = json.dumps(item).lower()
                if q in item_str:
                    key = item_str[:80]
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    results.append({
                        "platform": platform,
                        "collected_at": collected_at,
                        "item": item,
                    })

        return results
    except Exception as e:
        logger.error(f"search_raw_trends failed: {e}")
        return []


# ─────────────────────────────────────────────
# AI digest
# ─────────────────────────────────────────────

def save_ai_digest(payload: dict) -> bool:
    client = get_client()
    if not client:
        return False
    try:
        today = datetime.now().date().isoformat()
        gen_at = payload.get("timestamp") or datetime.now().isoformat()

        # Keep only one digest per calendar day: delete today's existing entries
        # before inserting the fresh one (same pattern as raw_trends)
        client.table("ai_digest").delete()\
            .gte("generated_at", today + "T00:00:00")\
            .lt("generated_at", today + "T23:59:59.999999")\
            .execute()

        client.table("ai_digest").insert({
            "trends": payload.get("trends", []),
            "generated_at": gen_at,
        }).execute()

        # Purge ai_digest entries older than 30 days
        cutoff = (datetime.now() - timedelta(days=30)).isoformat()
        client.table("ai_digest").delete().lt("generated_at", cutoff).execute()

        logger.info("AI digest saved (1 per day).")
        return True
    except Exception as e:
        logger.error(f"save_ai_digest failed: {e}")
        return False


def load_ai_digest() -> dict | None:
    client = get_client()
    if not client:
        return None
    try:
        resp = (
            client.table("ai_digest")
            .select("trends, generated_at")
            .order("generated_at", desc=True)
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        row = resp.data[0]
        return {"timestamp": row["generated_at"], "trends": row["trends"]}
    except Exception as e:
        logger.error(f"load_ai_digest failed: {e}")
        return None


# ─────────────────────────────────────────────
# Per-platform today check (IP protection)
# ─────────────────────────────────────────────

def has_platform_data_for_today(platform: str) -> bool:
    """Return True if raw_trends already has a row for this platform on today's date."""
    client = get_client()
    if not client:
        return False
    try:
        today = datetime.now().date().isoformat()
        resp = (
            client.table("raw_trends")
            .select("id")
            .eq("collected_date", today)
            .eq("platform", platform)
            .limit(1)
            .execute()
        )
        return bool(resp.data)
    except Exception as e:
        logger.error(f"has_platform_data_for_today({platform}) failed: {e}")
        return False


def load_platform_data_for_today(platform: str) -> list:
    """Return today's most-recent data list for a specific platform."""
    client = get_client()
    if not client:
        return []
    try:
        today = datetime.now().date().isoformat()
        resp = (
            client.table("raw_trends")
            .select("data")
            .eq("collected_date", today)
            .eq("platform", platform)
            .order("collected_at", desc=True)
            .limit(1)
            .execute()
        )
        if resp.data:
            return resp.data[0].get("data") or []
        return []
    except Exception as e:
        logger.error(f"load_platform_data_for_today({platform}) failed: {e}")
        return []


# ─────────────────────────────────────────────
# Competitor signals
# ─────────────────────────────────────────────

def save_competitor_signals(signals: list) -> bool:
    if not signals:
        return True
    client = get_client()
    if not client:
        return False
    try:
        client.table("competitor_signals").insert(signals).execute()

        # Fix 9: purge competitor_signals older than 30 days
        cutoff = (datetime.now() - timedelta(days=30)).isoformat()
        client.table("competitor_signals").delete().lt("collected_at", cutoff).execute()

        logger.info(f"Saved {len(signals)} competitor signals.")
        return True
    except Exception as e:
        logger.error(f"save_competitor_signals failed: {e}")
        return False


def load_competitor_signals(days: int = 7) -> list:
    client = get_client()
    if not client:
        return []
    try:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        resp = (
            client.table("competitor_signals")
            .select("competitor, platform, mention_text, source_url, collected_at")
            .gte("collected_at", cutoff)
            .order("collected_at", desc=True)
            .limit(100)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        logger.error(f"load_competitor_signals failed: {e}")
        return []


def get_trend_velocity(keyword: str, days: int = 30) -> list:
    """
    Daily raw mention counts for keyword over past N days.
    Groups raw_trends rows by collected_date and counts matching items.
    Used for the velocity sparkline in the trend detail drawer.
    """
    client = get_client()
    if not client:
        return []
    try:
        cutoff = (datetime.now() - timedelta(days=days)).date().isoformat()
        resp = (
            client.table("raw_trends")
            .select("collected_date, data")
            .gte("collected_date", cutoff)
            .order("collected_date", desc=False)
            .limit(500)
            .execute()
        )
        kw = keyword.lower()
        counts: dict = {}
        for row in (resp.data or []):
            date_str = row.get("collected_date", "")
            if not date_str:
                continue
            data = row.get("data") or []
            items = data if isinstance(data, list) else [data]
            for item in items:
                if kw in json.dumps(item).lower():
                    counts[date_str] = counts.get(date_str, 0) + 1

        # Fill complete date range with zeros
        result = []
        start_d = (datetime.now() - timedelta(days=days)).date()
        end_d   = datetime.now().date()
        current = start_d
        while current <= end_d:
            ds = current.isoformat()
            result.append({"date": ds, "count": counts.get(ds, 0)})
            current += timedelta(days=1)
        return result
    except Exception as e:
        logger.error(f"get_trend_velocity failed: {e}")
        return []


def get_digest_history(keyword: str, days: int = 30) -> dict:
    """
    How many daily AI digests in the past N days contained a trend matching keyword.
    Used to show the "Seen in X digests" signal on trend cards.
    """
    client = get_client()
    if not client:
        return {"appearances": 0, "digest_dates": []}
    try:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        resp = (
            client.table("ai_digest")
            .select("trends, generated_at")
            .gte("generated_at", cutoff)
            .order("generated_at", desc=True)
            .limit(30)
            .execute()
        )
        kw = keyword.lower()
        dates: list[str] = []
        for row in (resp.data or []):
            trends = row.get("trends") or []
            gen_at = row.get("generated_at", "")
            for trend in trends:
                text = (trend.get("trend_name", "") + " " + trend.get("context", "")).lower()
                if kw in text:
                    date_str = gen_at[:10]
                    if date_str not in dates:
                        dates.append(date_str)
                    break
        return {"appearances": len(dates), "digest_dates": sorted(dates)}
    except Exception as e:
        logger.error(f"get_digest_history failed: {e}")
        return {"appearances": 0, "digest_dates": []}


def get_competitor_digest(days: int = 7) -> dict:
    """
    Aggregate competitor_signals for the past N days, grouped by brand.
    Returns two dicts:
      - competitors: {brand_name: {count, platforms, recent_mentions}}
      - own_brands:  {brand_name: {count, platforms, recent_mentions}}
    HUL entries are identified by the "HUL:" prefix in the competitor field.
    """
    signals = load_competitor_signals(days=days)

    competitors: dict = {}
    own_brands: dict = {}

    for s in signals:
        raw = s.get("competitor", "")
        is_own = raw.startswith("HUL:")
        brand = raw[4:] if is_own else raw
        bucket = own_brands if is_own else competitors

        if brand not in bucket:
            bucket[brand] = {"count": 0, "platforms": set(), "recent_mentions": []}

        entry = bucket[brand]
        entry["count"] += 1
        entry["platforms"].add(s.get("platform", ""))
        if len(entry["recent_mentions"]) < 5:
            entry["recent_mentions"].append({
                "text": s.get("mention_text", "")[:200],
                "platform": s.get("platform"),
                "url": s.get("source_url"),
                "collected_at": s.get("collected_at"),
            })

    # Convert sets to sorted lists for JSON serialisation
    def _serialise(bucket: dict) -> dict:
        return {
            brand: {
                "count": v["count"],
                "platforms": sorted(v["platforms"]),
                "recent_mentions": v["recent_mentions"],
            }
            for brand, v in sorted(bucket.items(), key=lambda x: -x[1]["count"])
        }

    return {
        "competitors": _serialise(competitors),
        "own_brands": _serialise(own_brands),
    }
