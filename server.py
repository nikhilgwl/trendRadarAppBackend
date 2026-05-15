from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import json
import os
import re
import yaml
from datetime import datetime

from core_logic import collect_raw_data, generate_ai_summary, RAW_DATA_FILE, AI_SUMMARY_FILE
import db

# Fix 4: track whether a sync/generate is already running to avoid overlaps
_sync_running = False
_ai_running = False


async def _auto_sync_task():
    """
    Fix 4: Run immediately on startup, then every 24h.
    Previous version waited 24h before first run — fresh deployments had no data.
    """
    import logging
    _log = logging.getLogger("auto_sync")
    _log.info("Auto-sync: running initial collection on startup...")
    try:
        await collect_raw_data()
        _log.info("Auto-sync: initial collection complete.")
    except Exception as e:
        _log.error(f"Auto-sync initial run error: {e}")

    while True:
        try:
            await asyncio.sleep(24 * 3600)
            _log.info("Auto-sync: running daily collection...")
            await collect_raw_data()
            _log.info("Auto-sync: daily collection complete.")
        except asyncio.CancelledError:
            break
        except Exception as e:
            _log.error(f"Auto-sync error: {e}")
            await asyncio.sleep(3600)  # back off 1h on error


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_auto_sync_task())
    yield
    task.cancel()


app = FastAPI(title="Trend Radar API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
EMPTY_RAW   = {"google": [], "reddit": [], "rss": [], "social": [], "pinterest": [],
               "amazon": [], "twitter": [], "instagram": [], "nykaa": []}


@app.get("/api/trends/raw")
async def get_raw_trends():
    data = db.load_raw_trends()
    if data:
        return data
    path = os.path.join(BACKEND_DIR, RAW_DATA_FILE)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return EMPTY_RAW


@app.get("/api/trends/ai")
async def get_ai_summary():
    data = db.load_ai_digest()
    if data:
        return data
    path = os.path.join(BACKEND_DIR, AI_SUMMARY_FILE)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"trends": []}


# Fix 12: AI generation runs as a background task — returns immediately with
# the current digest so the frontend isn't left waiting 30-60s for Gemini.
@app.post("/api/trends/ai/generate")
async def trigger_ai_summary(background_tasks: BackgroundTasks):
    global _ai_running
    if not _ai_running:
        _ai_running = True

        async def _run():
            global _ai_running
            try:
                await generate_ai_summary()
            finally:
                _ai_running = False

        background_tasks.add_task(_run)

    # Return the current digest immediately so the UI stays responsive
    current = db.load_ai_digest()
    if current:
        return {**current, "status": "regenerating" if _ai_running else "ready"}
    path = os.path.join(BACKEND_DIR, AI_SUMMARY_FILE)
    if os.path.exists(path):
        with open(path) as f:
            return {**json.load(f), "status": "regenerating"}
    return {"trends": [], "status": "regenerating"}


@app.post("/api/sync")
async def trigger_sync(background_tasks: BackgroundTasks):
    global _sync_running
    if not _sync_running:
        _sync_running = True

        async def _run():
            global _sync_running
            try:
                await collect_raw_data()
            finally:
                _sync_running = False

        background_tasks.add_task(_run)
        return {"status": "Sync started"}
    return {"status": "Sync already running"}


@app.get("/api/search")
async def search_trends(q: str = Query(..., min_length=2)):
    results = db.search_raw_trends(q, days=180)
    return {"query": q, "count": len(results), "results": results}


@app.get("/api/search/relevant")
async def get_search_relevant(q: str = Query(..., min_length=2)):
    """
    Use Gemini to synthesise the top fuzzy-matched raw results into
    3-5 AI-digest-format trend cards most relevant to the query.
    """
    from brain.gemini_filter import search_synthesize
    raw_results = db.search_raw_trends(q, days=180)
    if not raw_results:
        return {"query": q, "trends": []}
    trends = await asyncio.to_thread(search_synthesize, q, raw_results)
    return {"query": q, "trends": trends}


@app.get("/api/competitor-signals")
async def get_competitor_signals(days: int = Query(7, ge=1, le=180)):
    signals = db.load_competitor_signals(days=days)
    return {"days": days, "count": len(signals), "signals": signals}


@app.get("/api/trends/velocity")
async def get_trend_velocity(keyword: str = Query(..., min_length=2), days: int = Query(30, ge=7, le=90)):
    data = db.get_trend_velocity(keyword=keyword, days=days)
    return {"keyword": keyword, "days": days, "data": data}


@app.get("/api/trends/history")
async def get_trend_history(keyword: str = Query(..., min_length=2), days: int = Query(30, ge=7, le=180)):
    return db.get_digest_history(keyword=keyword, days=days)


@app.get("/api/competitor-digest")
async def get_competitor_digest(days: int = Query(7, ge=1, le=180)):
    digest = db.get_competitor_digest(days=days)
    return {"days": days, **digest}


@app.get("/api/marketplace/summary")
async def get_marketplace_summary():
    """
    Aggregate latest Amazon + Nykaa bestseller data into a brand leaderboard.
    Tags each brand as HUL / competitor / other using config.yaml lists.
    Used by the Brand Health panel Marketplace Intelligence section.
    """
    raw = db.load_raw_trends()
    if not raw:
        return {"categories": {}, "hul_gap_categories": [], "data_date": None}

    # Load brand config
    config_path = os.path.join(BACKEND_DIR, "config.yaml")
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except Exception:
        config = {}

    hul_brands  = config.get("brand_portfolio", [])
    comp_brands = config.get("competitors", [])

    def _make_pattern(brands: list):
        if not brands:
            return None
        # Sort longest first so "The Derma Co" matches before "Co"
        escaped = sorted([re.escape(b) for b in brands], key=len, reverse=True)
        return re.compile(r'(' + '|'.join(escaped) + r')', re.IGNORECASE)

    hul_pat  = _make_pattern(hul_brands)
    comp_pat = _make_pattern(comp_brands)

    def _identify_brand(product_name: str):
        """Return (canonical_brand, brand_type) where type in hul|competitor|other."""
        if hul_pat:
            m = hul_pat.search(product_name)
            if m:
                matched_lower = m.group(0).lower()
                canon = next((b for b in hul_brands if b.lower() == matched_lower), m.group(0))
                return canon, "hul"
        if comp_pat:
            m = comp_pat.search(product_name)
            if m:
                matched_lower = m.group(0).lower()
                canon = next((b for b in comp_brands if b.lower() == matched_lower), m.group(0))
                return canon, "competitor"
        # Fallback: first capitalised word(s) as brand guess
        first = product_name.strip().split()[0] if product_name.strip() else "Unknown"
        return first, "other"

    # Accumulator: {category: {brand: entry}}
    agg: dict = {}

    for item in (raw.get("amazon") or []):
        name = (item.get("product_name") or "").strip()
        cat  = (item.get("category") or "Beauty").strip()
        rank = item.get("rank") or 99
        if not name:
            continue
        brand, btype = _identify_brand(name)
        agg.setdefault(cat, {})
        if brand not in agg[cat]:
            agg[cat][brand] = {"count": 0, "best_rank": 999, "products": [], "type": btype, "platforms": set()}
        e = agg[cat][brand]
        e["count"] += 1
        e["best_rank"] = min(e["best_rank"], rank)
        e["platforms"].add("Amazon")
        if len(e["products"]) < 4:
            e["products"].append({"name": name, "rank": rank, "source": "Amazon"})

    for item in (raw.get("nykaa") or []):
        name = (item.get("product_name") or item.get("title") or "").strip()
        cat  = (item.get("category") or "Skincare").strip()
        if not name:
            continue
        brand, btype = _identify_brand(name)
        agg.setdefault(cat, {})
        if brand not in agg[cat]:
            agg[cat][brand] = {"count": 0, "best_rank": 999, "products": [], "type": btype, "platforms": set()}
        e = agg[cat][brand]
        e["count"] += 1
        e["platforms"].add("Nykaa")
        if len(e["products"]) < 4:
            e["products"].append({"name": name, "rank": None, "source": "Nykaa"})

    # Serialise to JSON-safe dicts, sort each leaderboard by product_count desc
    serialized: dict = {}
    for cat, brands in agg.items():
        lb = []
        for brand, data in brands.items():
            lb.append({
                "brand":         brand,
                "type":          data["type"],          # "hul" | "competitor" | "other"
                "is_hul":        data["type"] == "hul",
                "product_count": data["count"],
                "best_rank":     data["best_rank"] if data["best_rank"] < 999 else None,
                "platforms":     sorted(data["platforms"]),
                "products":      data["products"],
            })
        lb.sort(key=lambda x: -x["product_count"])
        serialized[cat] = lb

    hul_gap_categories = [
        cat for cat, lb in serialized.items()
        if not any(e["is_hul"] for e in lb)
    ]

    return {
        "categories":          serialized,
        "hul_gap_categories":  hul_gap_categories,
        "data_date":           raw.get("timestamp"),
    }


@app.get("/api/status")
async def get_status():
    return {
        "status": "Online",
        "last_check": datetime.now().isoformat(),
        "sync_running": _sync_running,
        "ai_running": _ai_running,
    }


if __name__ == "__main__":
    import uvicorn
    os.chdir(BACKEND_DIR)
    uvicorn.run(app, host="0.0.0.0", port=8000)
