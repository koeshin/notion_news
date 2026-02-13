import os
import yaml
import datetime
from typing import List, Set, Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from ..models import ContentItem

def generate_canonical_id(video_id: str) -> str:
    return f"yt:{video_id}"

def search_youtube(api_key: str, query: str, max_results: int = 5, order: str = "date") -> List[dict]:
    try:
        youtube = build("youtube", "v3", developerKey=api_key)
        
        # Calculate date for "publishedAfter" if needed, but for now we just get relevance
        # RFC 3339 format: 2023-01-01T00:00:00Z
        
        request = youtube.search().list(
            part="snippet",
            q=query,
            type="video",
            maxResults=max_results,
            order=order, 
            relevanceLanguage="en"
        )
        response = request.execute()
        return response.get("items", [])
    except HttpError as e:
        print(f"YouTube API Error: {e}")
        return []
    except Exception as e:
        print(f"Error searching YouTube: {e}")
        return []

def resolve_channel_id(youtube, handle: str) -> Optional[str]:
    """Resolves a YouTube handle (e.g. @OpenAI) to its internal UC ID"""
    try:
        # Note: forHandle requires the '@' prefix
        request = youtube.channels().list(
            part="id",
            forHandle=handle
        )
        response = request.execute()
        items = response.get("items", [])
        if items:
            return items[0]["id"]
        return None
    except Exception as e:
        print(f"Error resolving handle {handle}: {e}")
        return None

def extract_channels(
    channels_config_path: str,
    api_key: str,
    seen_ids: Set[str],
    start_date: Optional[datetime.datetime] = None
) -> List[ContentItem]:
    if not api_key:
        print("WARNING: No YouTube API Key provided. Skipping channel extraction.")
        return []

    if not os.path.exists(channels_config_path):
        return []

    with open(channels_config_path, "r") as f:
        config = yaml.safe_load(f)
        
    channels = config.get("channels", [])
    if not channels:
        return []

    items = []
    youtube = build("youtube", "v3", developerKey=api_key)

    for channel in channels:
        if not channel.get("enabled", True):
            continue

        name = channel["name"]
        handle = channel.get("handle")
        channel_id = channel.get("channel_id")
        
        # 1. Resolve ID if missing
        if not channel_id and handle:
            print(f"Resolving channel ID for {handle}...")
            channel_id = resolve_channel_id(youtube, handle)
            if not channel_id:
                print(f"Skipping {name}: Could not resolve ID.")
                continue

        print(f"Fetching updates from: {name} (ID: {channel_id})...")
        
        try:
            # 2. Get the 'uploads' playlist ID
            ch_request = youtube.channels().list(
                part="contentDetails",
                id=channel_id
            )
            ch_response = ch_request.execute()
            ch_items = ch_response.get("items", [])
            if not ch_items:
                continue
            
            uploads_playlist_id = ch_items[0]['contentDetails']['relatedPlaylists']['uploads']

            # 3. Fetch videos from the uploads playlist
            # We iterate through pages if needed, but apply Early Stop
            next_page_token = None
            stop_monitoring = False
            
            while not stop_monitoring:
                pl_request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                pl_response = pl_request.execute()
                v_results = pl_response.get("items", [])
                
                if not v_results:
                    break
                
                for res in v_results:
                    video_id = res["contentDetails"]["videoId"]
                    c_id = generate_canonical_id(video_id)
                    
                    snippet = res["snippet"]
                    title = snippet["title"]
                    description = snippet["description"]
                    channel_title = snippet["channelTitle"]
                    publish_time = snippet["publishedAt"]
                    
                    # Parse date
                    try:
                        published_at = datetime.datetime.strptime(publish_time, "%Y-%m-%dT%H:%M:%SZ")
                    except ValueError:
                        published_at = datetime.datetime.now()

                    # Early Stop Check
                    if start_date and published_at < start_date:
                        print(f"  - Reached older items ({published_at}), stopping.")
                        stop_monitoring = True
                        break

                    if c_id in seen_ids:
                        continue

                    item = ContentItem(
                        canonical_id=c_id,
                        type="YouTube",
                        source=f"YT:{name}",
                        title=title,
                        url=f"https://www.youtube.com/watch?v={video_id}",
                        published_at=published_at,
                        raw_text=f"{description}\n\nChannel: {channel_title}",
                        video_id=video_id,
                        channel=channel_title,
                        people_matches=[name]
                    )
                    items.append(item)
                    seen_ids.add(c_id)

                next_page_token = pl_response.get("nextPageToken")
                if not next_page_token:
                    break
        except Exception as e:
            print(f"Error fetching channel {name}: {e}")
            
    return items

def extract_youtube(
    people_config_path: str, 
    api_key: str, 
    seen_ids: Set[str], 
    max_people_per_run: int = 3,
    order: str = "date",
    max_results_per_person: int = 3
) -> List[ContentItem]:

    
    if not api_key:
        print("WARNING: No YouTube API Key provided. Skipping YouTube extraction.")
        return []

    with open(people_config_path, "r") as f:
        config = yaml.safe_load(f)
        
    people = config.get("people", [])
    if not people:
        return []

    # Simple round-robin simulation or just pick first N for MVP
    # In a real stateful run, we would read the last index from state.
    # For MVP/Dry Run, we just take the first N.
    selected_people = people[:max_people_per_run]
    
    items = []
    
    for person in selected_people:
        name = person["name"]
        aliases = person.get("aliases", [])
        keywords = ["interview", "podcast", "talk"]
        
        # Construct a query. 
        # Strategy: "{Name} interview" is usually high signal.
        # Exclude shorts
        query = f'"{name}" interview -shorts'
        
        print(f"Searching YouTube for: {query}...")
        results = search_youtube(api_key, query, max_results=max_results_per_person, order=order)
        
        for result in results:
            video_id = result["id"]["videoId"]
            c_id = generate_canonical_id(video_id)
            
            if c_id in seen_ids:
                continue
                
            snippet = result["snippet"]
            title = snippet["title"]
            description = snippet["description"]
            channel_title = snippet["channelTitle"]
            publish_time = snippet["publishedAt"] # 2024-02-05T10:00:00Z
            
            # Simple alias check in title/description
            # (The search query already enforces the name, but this verifies it's not just a passing mention)
            # For MVP, we trust the search result relevance + name match
            
            # content enforcement
            # Check if any alias or name is in title (case insensitive)
            full_text = (title + " " + description).lower()
            match_found = False
            check_names = [name.lower()] + [a.lower() for a in aliases]
            
            for cn in check_names:
                if cn in full_text:
                    match_found = True
                    break
            
            # if not match_found:
            #     continue 
            
            # Parse date
            try:
                published_at = datetime.datetime.strptime(publish_time, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                published_at = datetime.datetime.now()

            item = ContentItem(
                canonical_id=c_id,
                type="YouTube",
                source="YouTube",
                title=title,
                url=f"https://www.youtube.com/watch?v={video_id}",
                published_at=published_at,
                raw_text=f"{description}\n\nChannel: {channel_title}",
                video_id=video_id,
                channel=channel_title,
                people_matches=[name]
            )
            items.append(item)
            seen_ids.add(c_id) # Mark as seen within this run
            
    return items
