import google.generativeai as genai
import json
import os
from typing import List, Dict, Any
from ..models import ContentItem, ProcessingResult

# Configure Gemini
# Using "gemini-2.0-flash-exp" or latest available flash model as "3.0" request
MODEL_NAME = "gemini-3-flash-preview" # Adjust if 3.0 is available specifically

def setup_gemini(api_key: str):
    genai.configure(api_key=api_key)

def process_batch(items: List[ContentItem], model_name: str = MODEL_NAME) -> List[ContentItem]:
    if not items:
        return []
    
    # Prepare prompt
    items_json = []
    for item in items:
        # Truncate content for context window sanity, though Flash has large context
        # 10k chars is plenty for a summary
        content_preview = item.raw_text[:10000] if item.raw_text else ""
        items_json.append({
            "id": item.canonical_id,
            "title": item.title,
            "source": item.source,
            "published_at": str(item.published_at),
            "content": content_preview
        })
    
    prompt = f"""
    You are an expert AI implementation analyst. Your task is to analyze the following list of AI news items (Articles/YouTube videos).
    
    INPUT DATA:
    {json.dumps(items_json, indent=2)}
    
    INSTRUCTIONS:
    For each item, generate a JSON object with the following fields:
    - id: The same ID from input.
    - summary: A concise 3-sentence summary of the content.
    - tags: A list of relevant tags (e.g., ["AI", "LLM", "Infrastructure", "policy", "Hardware", "Leadership"]).
    - importance: An integer score from 1-10 based on these criteria:
        * 9-10: Major model release (e.g., GPT-5), massive regulatory shift, or breakthrough.
        * 6-8: significant update, key person interview, new useful tool.
        * 3-5: minor update, general discussion.
        * 1-2: rumor, noise, specific nice-to-know.
    - key_entities: List of important people, companies, or models mentioned.
    - actionable_insight: One sentence on what a developer/researcher should DO or KNOW based on this.
    
    OUTPUT FORMAT:
    Return a valid JSON object with a single key "results" containing the list of analyzed items.
    Example:
    {{
      "results": [
        {{
          "id": "...",
          "summary": "...",
          "tags": ["..."],
          "importance": 8,
          "key_entities": ["..."],
          "actionable_insight": "..."
        }}
      ]
    }}
    """
    
    try:
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        
        result_json = json.loads(response.text)
        
    except Exception as e:
        print(f"Error with primary model {model_name}: {e}")
        fallback_model = "gemini-2.5-flash"
        print(f"Attempting fallback to {fallback_model}...")
        try:
            model = genai.GenerativeModel(fallback_model)
            response = model.generate_content(
                prompt,
                generation_config={"response_mime_type": "application/json"}
            )
            result_json = json.loads(response.text)
        except Exception as fallback_e:
            print(f"Error with fallback model {fallback_model}: {fallback_e}")
            # Return original items if failure
            return items

    # If we got here, we have successful result_json
    try:
        results_map = {r["id"]: r for r in result_json.get("results", [])}
        
        processed_items = []
        for item in items:
            if item.canonical_id in results_map:
                res = results_map[item.canonical_id]
                
                # Update item fields
                item.summary = res.get("summary")
                item.tags = res.get("tags", [])
                item.importance = res.get("importance", 0)
                item.key_entities = res.get("key_entities", [])
                item.actionable_insight = res.get("actionable_insight")
                
                processed_items.append(item)
            else:
                # If LLM missed it, keep original but maybe mark as unanalyzed?
                # For now just return it as is (or empty summary)
                print(f"Warning: LLM skipped item {item.canonical_id}")
                processed_items.append(item)
                
        return processed_items
        
    except Exception as parse_e:
        print(f"Error parsing LLM response maps: {parse_e}")
        return items

