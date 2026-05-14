"""
ScriptForge Backend — FastAPI Server
Runs on Render.com
"""

import os
import re
import time
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime

from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from supabase import create_client, Client
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── LOGGING ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
SUPABASE_URL    = os.environ.get("SUPABASE_URL", "https://pdezqdtfsukuokqpoyux.supabase.co")
SUPABASE_KEY    = os.environ.get("SUPABASE_SERVICE_KEY", "")
APIFY_TOKEN     = os.environ.get("APIFY_API_TOKEN", "")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def build_youtube():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

# ── LIFESPAN ──────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_pending_channels())
    yield
    task.cancel()

# ── APP ───────────────────────────────────────────────
app = FastAPI(title="ScriptForge Backend", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── MODELS ────────────────────────────────────────────
class IndexChannelRequest(BaseModel):
    channel_id: str
    channel_url: str
    user_id: str
    language: str = "hi"
    max_videos: int = 5

# ══════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════
@app.get("/")
def root():
    return {"status": "ScriptForge Backend Running ✅", "version": "1.0.0"}

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# ══════════════════════════════════════════════════════
# WEBHOOK
# ══════════════════════════════════════════════════════
@app.post("/webhook/channel-added")
async def channel_added_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload     = await request.json()
        record      = payload.get("record", {})
        channel_id  = record.get("id")
        channel_url = record.get("channel_url")
        user_id     = record.get("user_id")
        language    = record.get("language", "hi")

        if not all([channel_id, channel_url, user_id]):
            return {"status": "missing fields"}

        background_tasks.add_task(run_indexing_pipeline,
            channel_id=channel_id, channel_url=channel_url,
            user_id=user_id, language=language)
        return {"status": "indexing started", "channel_id": channel_id}
    except Exception as e:
        log.error(f"Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════
# MANUAL TRIGGER
# ══════════════════════════════════════════════════════
@app.post("/index-channel")
async def index_channel_manual(req: IndexChannelRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_indexing_pipeline,
        channel_id=req.channel_id, channel_url=req.channel_url,
        user_id=req.user_id, language=req.language, max_videos=req.max_videos)
    return {"status": "indexing started", "channel_id": req.channel_id}

@app.get("/channel-status/{channel_id}")
def channel_status(channel_id: str):
    result = sb.table("channels").select("*").eq("id", channel_id).single().execute()
    if result.data:
        return result.data
    raise HTTPException(status_code=404, detail="Channel not found")

# ══════════════════════════════════════════════════════
# POLLING
# ══════════════════════════════════════════════════════
async def poll_pending_channels():
    log.info("🔄 Polling started for pending channels...")
    while True:
        try:
            result  = sb.table("channels").select("*").eq("status", "processing").execute()
            pending = result.data or []
            if pending:
                log.info(f"Found {len(pending)} pending channels")
                for ch in pending:
                    await asyncio.to_thread(run_indexing_pipeline,
                        channel_id=ch["id"], channel_url=ch["channel_url"],
                        user_id=ch["user_id"], language=ch.get("language", "hi"))
        except Exception as e:
            log.error(f"Polling error: {e}")
        await asyncio.sleep(60)

# ══════════════════════════════════════════════════════
# CORE PIPELINE
# ══════════════════════════════════════════════════════
def run_indexing_pipeline(channel_id, channel_url, user_id, language="hi", max_videos=5):
    log.info(f"🚀 Starting pipeline for: {channel_url}")
    sb.table("channels").update({"status": "indexing"}).eq("id", channel_id).execute()

    try:
        videos = get_video_ids(channel_url, max_videos)
        log.info(f"   Found {len(videos)} videos")

        if not videos:
            sb.table("channels").update({"status": "error"}).eq("id", channel_id).execute()
            return

        success, all_text = 0, []

        for i, video in enumerate(videos):
            vid_id = video["id"]
            title  = video["title"]
            log.info(f"   [{i+1}/{len(videos)}] {title[:50]}")

            try:
                text = fetch_transcript(vid_id, language)
                if text:
                    cleaned    = clean_transcript(text)
                    word_count = len(cleaned.split())
                    save_transcript_to_db(user_id, channel_id, vid_id, title, cleaned, word_count, language)
                    save_transcript_to_storage(user_id, channel_id, vid_id, title, cleaned)
                    all_text.append(cleaned)
                    success += 1
                    log.info(f"      ✅ {word_count:,} words saved")
                else:
                    log.info(f"      ⚠️  No transcript available")
            except Exception as e:
                log.error(f"      ❌ Error: {e}")

            time.sleep(2)

        total_words = sum(len(t.split()) for t in all_text)
        sb.table("channels").update({
            "status": "indexed", "videos_indexed": success, "words_indexed": total_words
        }).eq("id", channel_id).execute()
        log.info(f"✅ Pipeline complete: {success} videos, {total_words:,} words")

    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
        sb.table("channels").update({"status": "error"}).eq("id", channel_id).execute()

# ══════════════════════════════════════════════════════
# FETCH TRANSCRIPT — Apify
# ══════════════════════════════════════════════════════
def fetch_transcript(video_id: str, language: str = "hi") -> str | None:
    """
    Fetch transcript using Apify.
    Tries multiple actors until one works.
    """
    import requests

    if not APIFY_TOKEN:
        log.error("APIFY_API_TOKEN not set")
        return None

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    headers   = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}

    # pintostudio~youtube-transcript-scraper — correct input format
    actors = [
        {
            "url": "https://api.apify.com/v2/acts/pintostudio~youtube-transcript-scraper/run-sync-get-dataset-items",
            "payload": {
                "videoUrl": video_url,
                "targetLanguage": language   # "hi" for Hindi, "en" for English
            }
        },
    ]

    for actor in actors:
        try:
            log.info(f"      Trying actor: {actor['url'].split('/acts/')[1].split('/run')[0]}")
            res = requests.post(actor["url"], json=actor["payload"], headers=headers, timeout=120)

            if res.status_code == 200:
                data = res.json()
                if data:
                    text = extract_text_from_apify(data)
                    if text:
                        log.info(f"      ✅ Got transcript via Apify")
                        return text
            else:
                log.warning(f"      Actor returned {res.status_code}: {res.text[:100]}")

        except Exception as e:
            log.warning(f"      Actor error: {e}")
            continue

    log.error(f"      All Apify actors failed for {video_id}")
    return None


def extract_text_from_apify(data) -> str | None:
    """Extract plain text from various Apify response formats."""
    item = data[0] if isinstance(data, list) and data else data
    if not item:
        return None

    # Format 1: list of transcript segments
    for key in ["transcript", "captions", "subtitles", "segments"]:
        if key in item and isinstance(item[key], list):
            parts = item[key]
            text  = " ".join([p.get("text", "") or p.get("content", "") for p in parts])
            if text.strip():
                return text.strip()

    # Format 2: plain text field
    for key in ["text", "content", "transcriptText", "full_text", "body"]:
        if key in item and isinstance(item[key], str) and item[key].strip():
            return item[key].strip()

    log.warning(f"      Unknown Apify format. Keys: {list(item.keys())}")
    return None

# ══════════════════════════════════════════════════════
# VIDEO FETCHING — YouTube Data API v3
# ══════════════════════════════════════════════════════
def _resolve_channel_id(youtube, channel_url: str) -> str | None:
    channel_url = channel_url.rstrip('/')

    match = re.search(r'/channel/(UC[\w-]+)', channel_url)
    if match:
        return match.group(1)

    handle_match = re.search(r'/@([\w.-]+)', channel_url)
    if handle_match:
        res   = youtube.channels().list(part="id", forHandle=handle_match.group(1)).execute()
        items = res.get("items", [])
        if items:
            return items[0]["id"]

    custom_match = re.search(r'/(?:c|user)/([\w.-]+)', channel_url)
    if custom_match:
        res   = youtube.channels().list(part="id", forUsername=custom_match.group(1)).execute()
        items = res.get("items", [])
        if items:
            return items[0]["id"]

    return None


def get_video_ids(channel_url: str, max_count: int = 5) -> list:
    youtube    = build_youtube()
    channel_id = _resolve_channel_id(youtube, channel_url)
    if not channel_id:
        return []

    ch_res   = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    ch_items = ch_res.get("items", [])
    if not ch_items:
        return []

    uploads_id = ch_items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
    if not uploads_id:
        return []

    log.info(f"   Uploads playlist: {uploads_id}")
    videos, next_token = [], None

    while len(videos) < max_count:
        res = youtube.playlistItems().list(
            part="snippet", playlistId=uploads_id,
            maxResults=min(50, max_count - len(videos)),
            pageToken=next_token
        ).execute()

        for item in res.get("items", []):
            snippet  = item.get("snippet", {})
            vid_id   = snippet.get("resourceId", {}).get("videoId")
            title    = snippet.get("title", "Unknown")
            if vid_id:
                videos.append({"id": vid_id, "title": title})

        next_token = res.get("nextPageToken")
        if not next_token:
            break

    log.info(f"   Fetched {len(videos)} videos via YouTube Data API v3")
    return videos[:max_count]

# ══════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════
def clean_transcript(text: str) -> str:
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\[.*?\]', '', text)
    return text.strip()

def save_transcript_to_db(user_id, channel_id, video_id, title, content, word_count, language):
    try:
        sb.table("transcripts").upsert({
            "user_id": user_id, "channel_id": channel_id, "video_id": video_id,
            "title": title, "content": content, "word_count": word_count, "language": language,
        }, on_conflict="video_id").execute()
    except Exception as e:
        log.error(f"DB save error: {e}")

def save_transcript_to_storage(user_id, channel_id, video_id, title, content):
    try:
        safe_title   = re.sub(r'[^\w\s\-]', '', title)[:40].strip()
        file_path    = f"{user_id}/{channel_id}/{video_id}_{safe_title}.txt"
        file_content = f"Title: {title}\nVideo ID: {video_id}\nURL: https://youtube.com/watch?v={video_id}\n{'─'*60}\n\n{content}"
        sb.storage.from_("transcripts").upload(
            path=file_path, file=file_content.encode('utf-8'),
            file_options={"content-type": "text/plain", "upsert": "true"}
        )
    except Exception as e:
        log.error(f"Storage save error: {e}")
