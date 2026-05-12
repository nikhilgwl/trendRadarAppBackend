from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from datetime import datetime

from core_logic import collect_raw_data, generate_ai_summary, RAW_DATA_FILE, AI_SUMMARY_FILE
import db

app = FastAPI(title="Trend Radar Web-App API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
EMPTY_RAW = {"google": [], "reddit": [], "rss": [], "social": [], "pinterest": []}


@app.get("/api/trends/raw")
async def get_raw_trends():
    # 1. Try Supabase first (survives server restarts)
    data = db.load_raw_trends()
    if data:
        return data

    # 2. Fall back to local JSON file
    path = os.path.join(BACKEND_DIR, RAW_DATA_FILE)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)

    return EMPTY_RAW


@app.get("/api/trends/ai")
async def get_ai_summary():
    # 1. Try Supabase first
    data = db.load_ai_digest()
    if data:
        return data

    # 2. Fall back to local JSON file
    path = os.path.join(BACKEND_DIR, AI_SUMMARY_FILE)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)

    return {"trends": []}


@app.post("/api/trends/ai/generate")
async def trigger_ai_summary():
    result = await generate_ai_summary()
    return result or {"trends": []}


@app.post("/api/sync")
async def trigger_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(collect_raw_data)
    return {"status": "Raw Data Sync started in background"}


@app.get("/api/status")
async def get_status():
    return {"status": "Online", "last_check": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn
    os.chdir(BACKEND_DIR)
    uvicorn.run(app, host="0.0.0.0", port=8000)
