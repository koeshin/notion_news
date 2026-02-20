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
    
    # Define "Today" with the EXACT CURRENT TIME
    # We will compute a threshold based on what the user means by "today's Date with current time".
    # Since they want to run it dynamically and "not ignore time", we will use the exact `datetime.now()` 
    # to represent the run-time, and typically filter for the last 24 hours.
    from datetime import timedelta
    current_time = datetime.now()
    cutoff_time = current_time - timedelta(hours=24) 
    print(f"  - Target Date/Time Threshold: items published after {cutoff_time}")
    
    rss_items = extract_rss(RSS_CONFIG, seen_ids, target_date=cutoff_time)
    print(f"  - Extracted {len(rss_items)} RSS items")
    
    youtube_items = []
    yt_api_key = os.getenv("YOUTUBE_API_KEY")
    if yt_api_key:
        # 1.1 Person-based search
        youtube_items = extract_youtube(PEOPLE_CONFIG, yt_api_key, seen_ids, target_date=cutoff_time, max_people_per_run=3)
        print(f"  - Extracted {len(youtube_items)} YouTube person search items")
        
        # 1.2 Channel-based monitor
        print(f"  - Monitoring channels since: {cutoff_time}")
        channel_items = extract_channels(CHANNELS_CONFIG, yt_api_key, seen_ids, start_date=cutoff_time)
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
    
    # Separate Stats Dictionaries
    rss_stats = {
        "found": len(rss_items),
        "processed": 0,
        "uploaded": 0,
        "errors": 0,
        "skipped_dry_run": 0
    }
    yt_stats = {
        "found": len(youtube_items),
        "processed": 0,
        "uploaded": 0,
        "errors": 0,
        "skipped_dry_run": 0
    }

    # Split items for different processing
    articles = [i for i in all_items if i.type == "Article"]
    videos = [i for i in all_items if i.type == "YouTube"]

    # Process Articles (with LLM)
    if articles:
        print(f"\n[Phase 2a] Processing {len(articles)} Articles (with LLM)")
        total_batches = (len(articles) + BATCH_SIZE - 1) // BATCH_SIZE
        
        for i in range(0, len(articles), BATCH_SIZE):
            batch = articles[i:i+BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            print(f"  > Batch {batch_num}/{total_batches} ({len(batch)} items)...")
            
            analyzed_batch = []
            if google_api_key:
                try:
                    analyzed_batch = process_batch(batch)
                except Exception as e:
                    print(f"    [LLM Error] {e} - Falling back to raw text.")
                    # IMPORTANT: If LLM fails, we MUST fallback to raw to ensure upload
                    analyzed_batch = batch 
            else:
                analyzed_batch = batch
            
            for item in analyzed_batch:
                rss_stats["processed"] += 1
                if loader:
                     print(f"    - Upserting: {item.title[:40]}...", end=" ")
                     status = loader.upsert_item(item)
                     print(f"[{status}]")
                     if status in ["created", "updated"]:
                        state["seen_canonical_ids"].append(item.canonical_id)
                        rss_stats["uploaded"] += 1
                     elif status == "error":
                        rss_stats["errors"] += 1
                else:
                     print(f"    - [Dry Run] {item.title[:40]}...")
                     rss_stats["skipped_dry_run"] += 1
            save_state(state)

    # Process Videos (Skip LLM)
    if videos:
        print(f"\n[Phase 2b] Processing {len(videos)} Videos (No LLM)")
        for item in videos:
            yt_stats["processed"] += 1
            
            item.importance = 3 
            
            if loader:
                 print(f"    - Upserting: {item.title[:40]}...", end=" ")
                 status = loader.upsert_item(item)
                 print(f"[{status}]")
                 if status in ["created", "updated"]:
                    state["seen_canonical_ids"].append(item.canonical_id)
                    yt_stats["uploaded"] += 1
                 elif status == "error":
                    yt_stats["errors"] += 1
            else:
                 print(f"    - [Dry Run] {item.title[:40]}...")
                 yt_stats["skipped_dry_run"] += 1
        
        save_state(state)

    # --- 4. Finalize ---
    state["last_run_at"] = datetime.now().isoformat()
    save_state(state)
    
    print("\n=========================")
    print("--- Pipeline Summary ---")
    print("=========================")
    print("[NEWS (RSS)]")
    print(f"Found:           {rss_stats['found']}")
    print(f"Processed:       {rss_stats['processed']}")
    print(f"Uploaded:        {rss_stats['uploaded']}")
    print(f"Errors:          {rss_stats['errors']}")
    if DRY_RUN or not loader:
        print(f"Dry Run Skipped: {rss_stats['skipped_dry_run']}")
    print("-------------------------")
    print("[YOUTUBE]")
    print(f"Found:           {yt_stats['found']}")
    print(f"Processed:       {yt_stats['processed']}")
    print(f"Uploaded:        {yt_stats['uploaded']}")
    print(f"Errors:          {yt_stats['errors']}")
    if DRY_RUN or not loader:
        print(f"Dry Run Skipped: {yt_stats['skipped_dry_run']}")
    print("=========================")
    print("\n--- Pipeline Finished ---")

if __name__ == "__main__":
    main()
