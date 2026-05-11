import json
import os
from datetime import datetime, timedelta

DB_FILE = "seen_trends.json"

def is_new_trend(trend_name, interval_minutes=720):
    """
    Check if a trend has been seen in the last 'interval_minutes' (default 12 hours).
    Auto-purges entries older than 24 hours.
    """
    now = datetime.now()
    data = {}

    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
                data = json.load(f)
        except:
            data = {}

    # Purge old entries (> 24 hours)
    cutoff_24h = now - timedelta(hours=24)
    data = {k: v for k, v in data.items() if datetime.fromisoformat(v) > cutoff_24h}

    # Check the configurable window for the current trend
    is_new = True
    cutoff_window = now - timedelta(minutes=interval_minutes)
    
    if trend_name in data:
        last_seen = datetime.fromisoformat(data[trend_name])
        if last_seen > cutoff_window:
            is_new = False

    # Update timestamp if it's new or if we want to refresh the window
    if is_new:
        data[trend_name] = now.isoformat()
        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=4)

    return is_new
