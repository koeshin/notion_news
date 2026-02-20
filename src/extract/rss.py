import feedparser
import hashlib
from datetime import datetime
from typing import List
import yaml
from ..models import ContentItem

def generate_canonical_id(url: str) -> str:
    hash_val = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return f"rss:{hash_val}"

def parse_date(entry) -> datetime:
    # feedparser handles many date formats in 'published_parsed'
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6])
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return datetime(*entry.updated_parsed[:6])
    return datetime.now()

def extract_rss(sources_path: str, seen_ids: set, target_date: datetime = None) -> List[ContentItem]:
    with open(sources_path, "r") as f:
        config = yaml.safe_load(f)
    
    items = []
    
    items = []

    for source in config.get("sources", []):
        # Skip commented out or empty sources (yaml parser might return None for empty ones, but here we iterate list)
        if not source.get("url"): 
            continue
            
        print(f"Fetching {source['name']}...")
        try:
            feed = feedparser.parse(source["url"])
            
            total_entries = len(feed.entries)
            skipped_duplicates = 0
            skipped_old = 0
            
            for entry in feed.entries:
                url = entry.get("link", "")
                if not url:
                    continue
                
                # Check date first
                pub_date = parse_date(entry)
                # target_date is already a datetime object (or None)
                if target_date and pub_date < target_date:
                    skipped_old += 1
                    continue

                c_id = generate_canonical_id(url)
                if c_id in seen_ids:
                    skipped_duplicates += 1
                    continue
                
                
                # Basic content extraction
                # Prefer content > summary > description
                content = ""
                if "content" in entry:
                    content = entry.content[0].value
                elif "summary" in entry:
                    content = entry.summary
                else:
                    content = entry.get("description", "")

                item = ContentItem(
                    canonical_id=c_id,
                    type="Article",
                    source=source["name"],
                    title=entry.get("title", "No Title"),
                    url=url,
                    published_at=pub_date,
                    raw_text=content
                )
                items.append(item)

            print(f"    -> Found {total_entries} entries, Skipped {skipped_duplicates} duplicates, Skipped {skipped_old} old, New {len(items) - (len(items) - (total_entries - skipped_duplicates - skipped_old)) if total_entries > 0 else 0}")
                
        except Exception as e:
            print(f"Error fetching {source['name']}: {e}")
            
    return items
