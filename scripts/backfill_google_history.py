"""
One-time backfill: pulls Google Trends interest-over-time data for the past 6 months
and inserts it into the raw_trends table with the correct historical collected_date.

Only Google Trends has a free historical API. All other platforms start accumulating
forward from the first daily sync.

Run from backend/ directory:  python scripts/backfill_google_history.py
"""
import sys
import os
import time
import random
import logging
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BEAUTY_KEYWORDS = [
    "vitamin c serum", "niacinamide", "retinol", "sunscreen india",
    "hair fall treatment", "scalp serum", "rosemary hair oil",
    "hyaluronic acid", "ceramide moisturizer", "face wash india",
    "hair mask", "dandruff shampoo", "skin brightening", "under eye cream",
    "onion hair oil", "biotin hair", "kojic acid", "salicylic acid face wash",
    "anti ageing cream india", "spf 50 sunscreen",
]

# Chunk into groups of 5 (pytrends limit per request)
CHUNK_SIZE = 5
WEEKS_BACK = 26  # 6 months


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def make_pytrends():
    from pytrends.request import TrendReq
    return TrendReq(
        hl="en-IN", tz=330, timeout=(10, 30), retries=2, backoff_factor=1.0,
        requests_args={
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Cookie": "CONSENT=YES+cb.20210328-17-p0.en-GB+FX+719;",
            }
        }
    )


def backfill():
    import db
    client = db.get_client()
    if not client:
        logger.error("No Supabase client — set SUPABASE_URL and SUPABASE_KEY in .env")
        return

    today = datetime.now().date()
    inserted_total = 0

    for kw_chunk in chunks(BEAUTY_KEYWORDS, CHUNK_SIZE):
        logger.info(f"Fetching 6-month history for: {kw_chunk}")
        try:
            pt = make_pytrends()
            pt.build_payload(kw_chunk, timeframe="today 12-m", geo="IN")
            iot = pt.interest_over_time()

            if iot.empty:
                logger.warning(f"No data for chunk {kw_chunk}")
                time.sleep(random.uniform(5, 8))
                continue

            # Group by week: each row in iot is a weekly data point
            rows_to_insert = []
            for ts, row_data in iot.iterrows():
                week_date = ts.date().isoformat()
                # Build a snapshot list of {query, interest} for that week
                snapshot = [
                    {"query": kw, "interest": int(row_data[kw]), "period": week_date}
                    for kw in kw_chunk
                    if kw in row_data and int(row_data[kw]) > 0
                ]
                if snapshot:
                    rows_to_insert.append({
                        "platform":       "google",
                        "data":           snapshot,
                        "collected_at":   f"{week_date}T00:00:00+00:00",
                        "collected_date": week_date,
                    })

            if rows_to_insert:
                # Insert in batches of 50
                for i in range(0, len(rows_to_insert), 50):
                    batch = rows_to_insert[i:i + 50]
                    client.table("raw_trends").insert(batch).execute()
                    inserted_total += len(batch)
                    logger.info(f"  Inserted {inserted_total} rows so far...")

        except Exception as e:
            logger.error(f"Error for chunk {kw_chunk}: {e}")

        # Polite delay between chunks to avoid 429
        sleep_s = random.uniform(8, 15)
        logger.info(f"Sleeping {sleep_s:.1f}s before next chunk...")
        time.sleep(sleep_s)

    logger.info(f"Backfill complete. Total rows inserted: {inserted_total}")


if __name__ == "__main__":
    backfill()
