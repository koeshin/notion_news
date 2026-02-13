import os
import requests
from typing import Optional, List, Dict, Any
from datetime import datetime
from ..models import ContentItem

class NotionLoader:
    def __init__(self, token: str, database_id: str):
        self.token = token
        self.database_id = self._format_uuid(database_id)
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        self.base_url = "https://api.notion.com/v1"

    def _format_uuid(self, uuid_str: str) -> str:
        if len(uuid_str) == 32 and "-" not in uuid_str:
            return f"{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}"
        return uuid_str

    def find_page_by_canonical_id(self, canonical_id: str) -> Optional[str]:
        """Returns page_id if found, else None"""
        url = f"{self.base_url}/databases/{self.database_id}/query"
        payload = {
            "filter": {
                "property": "CanonicalId",
                "rich_text": {
                    "equals": canonical_id
                }
            }
        }
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            if response.status_code != 200:
                print(f"Error finding page: {response.status_code} {response.text}")
                return None
                
            results = response.json().get("results", [])
            if results:
                return results[0]["id"]
            return None
        except Exception as e:
            print(f"Error querying Notion: {e}")
            return None

    def clear_database(self):
        """Archives all pages in the database to clear it."""
        print(f"Clearing database {self.database_id}...")
        has_more = True
        next_cursor = None
        
        archived_count = 0
        url = f"{self.base_url}/databases/{self.database_id}/query"
        
        while has_more:
            try:
                payload = {"page_size": 100}
                if next_cursor:
                    payload["start_cursor"] = next_cursor
                
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
                if response.status_code != 200:
                    print(f"Error clearing DB query: {response.text}")
                    break
                    
                data = response.json()
                results = data.get("results", [])
                
                for page in results:
                    page_id = page["id"]
                    # Archive page
                    update_url = f"{self.base_url}/pages/{page_id}"
                    up_resp = requests.patch(update_url, headers=self.headers, json={"archived": True}, timeout=30)
                    if up_resp.status_code == 200:
                        archived_count += 1
                        print(f"  - Archived page {page_id}")
                    else:
                        print(f"  - Failed to archive {page_id}: {up_resp.status_code}")
                    
                has_more = data.get("has_more", False)
                next_cursor = data.get("next_cursor")
            except Exception as e:
                print(f"Error clearing database: {e}")
                break
        
        print(f"Database cleared. Archived {archived_count} pages.")

    def upsert_item(self, item: ContentItem):
        page_id = self.find_page_by_canonical_id(item.canonical_id)
        
        properties = {
            "Title": {"title": [{"text": {"content": item.title[:2000]}}]},
            "URL": {"url": item.url},
            "Source": {"select": {"name": item.source}},
            "Type": {"select": {"name": item.type}},
            "PublishedAt": {"date": {"start": item.published_at.isoformat()}},
            "IngestedAt": {"date": {"start": datetime.now().isoformat()}},
            "Importance": {"number": item.importance},
            "CanonicalId": {"rich_text": [{"text": {"content": item.canonical_id}}]},
        }
        
        # Optional fields
        if item.summary:
            properties["Summary"] = {"rich_text": [{"text": {"content": item.summary[:2000]}}]}
            
        if item.actionable_insight:
            properties["ActionableInsight"] = {"rich_text": [{"text": {"content": item.actionable_insight[:2000]}}]}

        if item.tags:
            # Multi-select has limits, so maybe just top 3-5 or ensure they exist
            # For MVP, proper error handling for new tags is needed if db doesn't allow creation
            # But usually API allows creating new options if configured.
            tags_objs = [{"name": t.replace(",", "")} for t in item.tags[:10]] 
            properties["Tags"] = {"multi_select": tags_objs}
            
        if item.people_matches:
             properties["PeopleMatches"] = {"multi_select": [{"name": p} for p in item.people_matches]}
        
        # YouTube specific
        if item.video_id:
             properties["VideoId"] = {"rich_text": [{"text": {"content": item.video_id}}]}
        if item.channel:
             properties["Channel"] = {"rich_text": [{"text": {"content": item.channel}}]}


        try:
            if page_id:
                # Update
                update_url = f"{self.base_url}/pages/{page_id}"
                resp = requests.patch(update_url, headers=self.headers, json={"properties": properties}, timeout=30)
                if resp.status_code == 200:
                    return "updated"
                else:
                    print(f"Error updating Notion ({resp.status_code}): {resp.text}")
                    return "error"
            else:
                # Create
                properties["CanonicalId"] = {"rich_text": [{"text": {"content": item.canonical_id}}]}
                create_url = f"{self.base_url}/pages"
                payload = {
                    "parent": {"database_id": self.database_id},
                    "properties": properties
                }
                resp = requests.post(create_url, headers=self.headers, json=payload, timeout=30)
                if resp.status_code == 200:
                    return "created"
                else:
                    print(f"Error creating Notion page ({resp.status_code}): {resp.text}")
                    return "error"
        except Exception as e:
            print(f"Error upserting to Notion ({item.title}): {e}")
            return "error"
