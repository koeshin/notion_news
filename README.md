# Notion Newsroom

Automated AI news aggregation pipeline.

## 1. Tracked Sources

### News (RSS)
- OpenAI
- DeepMind
- Microsoft AI
- AWS Machine Learning
- Hacker News (AI)
- Anthropic Engineering

### People (YouTube/Web)
- **OpenAI**: Sam Altman, Mark Chen, Jakub Pachocki
- **Anthropic**: Dario Amodei, Rahul Patil
- **Google DeepMind**: Demis Hassabis
- **Microsoft**: Kevin Scott

### YouTube Channels
- OpenAI
- Anthropic
- Google DeepMind

## 2. Methodology & Framework

### Workflow
1. **Extraction**:
   - Fetches RSS feeds for latest blog posts.
   - Monitors YouTube channels for new uploads (Early Stop pattern).
   - Searches YouTube for recent interviews of tracked people.
2. **Processing**:
   - Deduplicates content using canonical IDs.
   - Summarizes and tags content using **Gemini 3.0 Flash**.
   - Filters out Shorts and irrelevant content.
3. **Loading**:
   - Upserts organized data into **Notion Database**.
   - Streaming architecture for handling large batches.

### Tech Stack
- **Languages**: Python 3.11
- **Automation**: GitHub Actions (Scheduled every 6 hours)
- **Database**: Notion
- **AI Model**: Gemini 3.0 Flash
- **APIs**: YouTube Data API v3, Google Gen AI SDK, Notion API

## 3. Setup

### Prerequisites
- Python 3.9+
- Notion Integration Token & Database ID
- Google AI Studio API Key
- YouTube Data API Key

### Installation

```bash
git clone https://github.com/koeshin/notion_news.git
cd notion_news
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configuration
Set the following environment variables (or GitHub Secrets):
- NOTION_TOKEN
- NOTION_DATABASE_ID
- GOOGLE_API_KEY
- YOUTUBE_API_KEY

### Usage

**Manual Run**:
```bash
python main.py
```

**Backfill (Feb 5, 2026+)**:
```bash
python debug/clean_and_backfill.py
```
