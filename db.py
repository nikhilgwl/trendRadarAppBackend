import os
import logging
from supabase import create_client, Client

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")  # Service Role key set in Render env vars

_client: Client | None = None

def get_client() -> Client | None:
    """Return a cached Supabase client, or None if credentials are missing."""
    global _client
    if _client:
        return _client
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase credentials not set. Falling back to local JSON files.")
        return None
    try:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
        return _client
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        return None


def save_raw_trends(payload: dict) -> bool:
    """
    Upsert raw trends into the raw_trends table.
    Stores each platform as a separate row for clean querying.
    Returns True on success.
    """
    client = get_client()
    if not client:
        return False
    try:
        for platform, data in payload.items():
            if platform == "timestamp":
                continue
            client.table("raw_trends").upsert({
                "platform": platform,
                "data": data,
                "collected_at": payload.get("timestamp")
            }, on_conflict="platform").execute()
        logger.info("Raw trends saved to Supabase successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to save raw trends to Supabase: {e}")
        return False


def load_raw_trends() -> dict | None:
    """
    Load the latest raw trends from Supabase.
    Returns a dict keyed by platform, or None on failure.
    """
    client = get_client()
    if not client:
        return None
    try:
        response = client.table("raw_trends").select("platform, data, collected_at").execute()
        if not response.data:
            return None
        result = {}
        timestamp = None
        for row in response.data:
            result[row["platform"]] = row["data"]
            if not timestamp:
                timestamp = row.get("collected_at")
        result["timestamp"] = timestamp
        return result
    except Exception as e:
        logger.error(f"Failed to load raw trends from Supabase: {e}")
        return None


def save_ai_digest(payload: dict) -> bool:
    """
    Insert a new AI digest into the ai_digest table.
    Returns True on success.
    """
    client = get_client()
    if not client:
        return False
    try:
        client.table("ai_digest").insert({
            "trends": payload.get("trends", []),
            "generated_at": payload.get("timestamp")
        }).execute()
        logger.info("AI digest saved to Supabase successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to save AI digest to Supabase: {e}")
        return False


def load_ai_digest() -> dict | None:
    """
    Load the most recent AI digest from Supabase.
    Returns a dict with trends list, or None on failure.
    """
    client = get_client()
    if not client:
        return None
    try:
        response = (
            client.table("ai_digest")
            .select("trends, generated_at")
            .order("generated_at", desc=True)
            .limit(1)
            .execute()
        )
        if not response.data:
            return None
        row = response.data[0]
        return {"timestamp": row["generated_at"], "trends": row["trends"]}
    except Exception as e:
        logger.error(f"Failed to load AI digest from Supabase: {e}")
        return None
