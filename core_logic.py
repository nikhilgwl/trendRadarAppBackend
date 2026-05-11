import asyncio
import yaml
import logging
import json
import os
from datetime import datetime
from collectors.google_trends import get_google_trends
from collectors.reddit_public_collector import get_reddit_trends
from collectors.rss_collector import get_rss_trends
from collectors.social_collector import get_social_trends
from collectors.pinterest_collector import get_pinterest_trends
from brain.gemini_filter import get_categorized_trends

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("run_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

RAW_DATA_FILE = "raw_trends.json"
AI_SUMMARY_FILE = "daily_beauty_insights.json"

async def collect_raw_data():
    """
    Collects raw trends from all platforms and saves them independently.
    """
    logger.info("Starting Raw Data Collection...")
    
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config.yaml: {e}")
        return None

    # Run Collectors
    results = await asyncio.gather(
        asyncio.to_thread(get_google_trends),
        asyncio.to_thread(get_reddit_trends, config),
        asyncio.to_thread(get_rss_trends, [r for c in config['categories'] for r in c['rss']]),
        asyncio.to_thread(get_social_trends, config),
        asyncio.to_thread(get_pinterest_trends, config)
    )

    google, reddit, rss, social, pinterest = results
    
    raw_payload = {
        "timestamp": datetime.now().isoformat(),
        "google": google,
        "reddit": reddit,
        "rss": rss,
        "social": social,
        "pinterest": pinterest
    }

    with open(RAW_DATA_FILE, "w") as f:
        json.dump(raw_payload, f, indent=4)
    
    logger.info("Raw data collection complete.")
    return raw_payload

async def generate_ai_summary():
    """
    Runs Gemini only on request to consolidate the latest raw data.
    """
    logger.info("Generating AI Consolidated Summary...")
    
    if not os.path.exists(RAW_DATA_FILE):
        logger.warning("No raw data found. Collecting now...")
        raw_payload = await collect_raw_data()
    else:
        with open(RAW_DATA_FILE, "r") as f:
            raw_payload = json.load(f)

    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        return None

    trends = get_categorized_trends(
        config, 
        raw_payload["google"], 
        raw_payload["reddit"], 
        raw_payload["rss"], 
        raw_payload["social"], 
        raw_payload["pinterest"]
    )
    
    if trends:
        ai_payload = {"timestamp": datetime.now().isoformat(), "trends": trends}
        with open(AI_SUMMARY_FILE, "w") as f:
            json.dump(ai_payload, f, indent=4)
        logger.info("AI Summary generated successfully.")
        return ai_payload
    
    return None

async def run_pipeline():
    """Default entry point for full sync."""
    await collect_raw_data()
    # We DON'T call generate_ai_summary here to save costs.
    # It will be called via its own API endpoint.

if __name__ == "__main__":
    asyncio.run(run_pipeline())
