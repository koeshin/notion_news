import os
import json
import yaml
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Set

from src.models import ContentItem
from src.extract.rss import extract_rss
from src.extract.youtube import extract_youtube, extract_channels
from src.transform.llm import setup_gemini, process_batch
from src.load.notion import NotionLoader

# Load env
load_dotenv()

DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"
STATE_FILE = "state/state.json"
RSS_CONFIG = "config/sources.yaml"
PEOPLE_CONFIG = "config/people.yaml"
CHANNELS_CONFIG = "config/channels.yaml"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"seen_canonical_ids": [], "last_run_at": None, "youtube_round_robin_index": 0}

def save_state(state):
    if DRY_RUN:
        print("[DRY_RUN] Skipping state save.")
        return
    
    # Keep seen_ids size manageable, e.g., last 5000
    if len(state["seen_canonical_ids"]) > 5000:
        state["seen_canonical_ids"] = state["seen_canonical_ids"][-5000:]
        
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def main():
    print(f"--- Notion Newsroom Pipeline Started (DRY_RUN={DRY_RUN}) ---")
    
    state = load_state()
    seen_ids = set(state.get("seen_canonical_ids", []))
    
    # --- 1. Extract ---
    print("\n[Phase 1] Extraction")
    rss_items = extract_rss(RSS_CONFIG, seen_ids)
    print(f"  - Extracted {len(rss_items)} RSS items")
    
    youtube_items = []
    yt_api_key = os.getenv("YOUTUBE_API_KEY")
    if yt_api_key:
        # 1.1 Person-based search
        youtube_items = extract_youtube(PEOPLE_CONFIG, yt_api_key, seen_ids, max_people_per_run=3)
        print(f"  - Extracted {len(youtube_items)} YouTube person search items")
        
        # 1.2 Channel-based monitor
        # Use last_run_at from state or default to 24h ago
        last_run_iso = state.get("last_run_at")
        start_date = None
        if last_run_iso:
            start_date = datetime.fromisoformat(last_run_iso)
        else:
            from datetime import timedelta
            start_date = datetime.now() - timedelta(days=1)
            
        print(f"  - Monitoring channels since: {start_date}")
        channel_items = extract_channels(CHANNELS_CONFIG, yt_api_key, seen_ids, start_date=start_date)
        print(f"  - Extracted {len(channel_items)} YouTube channel items")
        youtube_items.extend(channel_items)
    else:
        print("  - Skipping YouTube (No API Key)")
        
    all_items = rss_items + youtube_items
    print(f"  - Total new items: {len(all_items)}")
    
    if not all_items:
        print("No new items to process. Exiting.")
        return

    # --- 2. Transform & Load (Streaming) ---
    print("\n[Phase 2 & 3] Transformation & Loading")
    
    google_api_key = os.getenv("GOOGLE_API_KEY")
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_DATABASE_ID")
    
    # Setup Notion Loader
    loader = None
    if notion_token and database_id and not DRY_RUN:
        loader = NotionLoader(notion_token, database_id)
        
    if google_api_key:
        setup_gemini(google_api_key)
    else:
        print("  - Warning: No Google API Key. Skipping LLM analysis (using raw content).")

    BATCH_SIZE = 10
    total_batches = (len(all_items) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(all_items), BATCH_SIZE):
        batch = all_items[i:i+BATCH_SIZE]
        batch_num = i//BATCH_SIZE + 1
        print(f"\n  > Batch {batch_num}/{total_batches} ({len(batch)} items)...")
        
        # 2.1 Transform
        if google_api_key:
            try:
                analyzed_batch = process_batch(batch)
            except Exception as e:
                print(f"    [LLM Error] {e}")
                analyzed_batch = batch # Fallback to raw
        else:
            analyzed_batch = batch

        # 2.2 Load
        for item in analyzed_batch:
            if loader:
                print(f"    - Upserting: {item.title[:40]}...", end=" ")
                status = loader.upsert_item(item)
                print(f"[{status}]")
                if status in ["created", "updated"]:
                    state["seen_canonical_ids"].append(item.canonical_id)
            else:
                 print(f"    - [Dry Run/No Loader] {item.title[:40]}... (Imp: {item.importance})")
        
        # Save state periodically (every batch)
        save_state(state)

    # --- 4. Finalize ---
    state["last_run_at"] = datetime.now().isoformat()
    save_state(state)
    print("\n--- Pipeline Finished ---")

if __name__ == "__main__":
    main()
