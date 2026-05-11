from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from datetime import datetime

# Import the local core_logic
from core_logic import collect_raw_data, generate_ai_summary, RAW_DATA_FILE, AI_SUMMARY_FILE

app = FastAPI(title="Trend Radar Web-App API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))

@app.get("/api/trends/raw")
async def get_raw_trends():
    path = os.path.join(BACKEND_DIR, RAW_DATA_FILE)
    if not os.path.exists(path):
        return {"google": [], "reddit": [], "rss": [], "social": [], "pinterest": []}
    
    with open(path, "r") as f:
        return json.load(f)

@app.get("/api/trends/ai")
async def get_ai_summary():
    path = os.path.join(BACKEND_DIR, AI_SUMMARY_FILE)
    if not os.path.exists(path):
        # Trigger it once if it doesn't exist
        return await generate_ai_summary()
    
    with open(path, "r") as f:
        return json.load(f)

@app.post("/api/trends/ai/generate")
async def trigger_ai_summary():
    # Force generate a new AI summary
    result = await generate_ai_summary()
    return result

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
