from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import datetime

class ContentItem(BaseModel):
    canonical_id: str = Field(..., description="Unique ID: rss:<hash> or yt:<videoId>")
    type: Literal["Article", "YouTube"]
    source: str
    title: str
    url: str
    published_at: datetime
    raw_text: Optional[str] = None
    summary: Optional[str] = None
    tags: List[str] = []
    importance: int = Field(0, ge=0, le=10)
    key_entities: List[str] = []
    people_matches: List[str] = []
    actionable_insight: Optional[str] = None
    
    # YouTube specific
    video_id: Optional[str] = None
    channel: Optional[str] = None

class ProcessingResult(BaseModel):
    items: List[ContentItem]
    processed_count: int
    duplicate_count: int
    error_count: int
