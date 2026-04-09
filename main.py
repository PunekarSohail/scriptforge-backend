"""
ScriptForge Backend — FastAPI Server
Runs on Railway.app
Handles: channel indexing, transcript storage, webhook from Supabase
"""

import os
import re
import time
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime

import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi
from supabase import create_client, Client
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── LOGGING ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# ── SUPABASE ──────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://pdezqdtfsukuokqpoyux.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")  # Use SERVICE key for backend
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── APP ───────────────────────────────────────────────
app = FastAPI(title="ScriptForge Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── MODELS ────────────────────────────────────────────
class IndexChannelRequest(BaseModel):
    channel_id: str      # UUID from channels table
    channel_url: str
    user_id: str
    language: str = "hi"
    max_videos: int = 50

class WebhookPayload(BaseModel):
    type: str
    table: str
    record: dict
    old_record: dict = {}

# ══════════════════════════════════════════════════════
# HEALTH CHECK
# ══════════════════════════════════════════════════════
@app.get("/")
def root():
    return {"status": "ScriptForge Backend Running ✅", "version": "1.0.0"}

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# ══════════════════════════════════════════════════════
# WEBHOOK — Supabase calls this when a new channel is added
# ══════════════════════════════════════════════════════
@app.post("/webhook/channel-added")
async def channel_added_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Supabase Database Webhook triggers this when a new row is inserted in 'channels' table.
    We then kick off the indexing pipeline in the background.
    """
    try:
        payload = await request.json()
        log.info(f"Webhook received: {payload.get('type')} on {payload.get('table')}")

        record = payload.get("record", {})
        if not record:
            return {"status": "no record"}

        channel_id  = record.get("id")
        channel_url = record.get("channel_url")
        user_id     = record.get("user_id")
        language    = record.get("language", "hi")

        if not all([channel_id, channel_url, user_id]):
            log.warning("Missing required fields in webhook payload")
            return {"status": "missing fields"}

        # Start indexing in background — don't block the webhook response
        background_tasks.add_task(
            run_indexing_pipeline,
            channel_id=channel_id,
            channel_url=channel_url,
            user_id=user_id,
            language=language
        )

        return {"status": "indexing started", "channel_id": channel_id}

    except Exception as e:
        log.error(f"Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════
# MANUAL TRIGGER — in case webhook fails
# ══════════════════════════════════════════════════════
@app.post("/index-channel")
async def index_channel_manual(req: IndexChannelRequest, background_tasks: BackgroundTasks):
    """Manual trigger to index a channel."""
    background_tasks.add_task(
        run_indexing_pipeline,
        channel_id=req.channel_id,
        channel_url=req.channel_url,
        user_id=req.user_id,
        language=req.language,
        max_videos=req.max_videos
    )
    return {"status": "indexing started", "channel_id": req.channel_id}


@app.get("/channel-status/{channel_id}")
def channel_status(channel_id: str):
    """Check indexing status of a channel."""
    result = sb.table("channels").select("*").eq("id", channel_id).single().execute()
    if result.data:
        return result.data
    raise HTTPException(status_code=404, detail="Channel not found")


# ══════════════════════════════════════════════════════
# POLLING — checks for unprocessed channels every 60s
# ══════════════════════════════════════════════════════
@app.on_event("startup")
async def start_polling():
    """Start background polling for unprocessed channels."""
    asyncio.create_task(poll_pending_channels())

async def poll_pending_channels():
    """Poll every 60 seconds for channels with 'processing' status."""
    log.info("🔄 Polling started for pending channels...")
    while True:
        try:
            result = sb.table("channels").select("*").eq("status", "processing").execute()
            pending = result.data or []
            if pending:
                log.info(f"Found {len(pending)} pending channels")
                for ch in pending:
                    await asyncio.to_thread(
                        run_indexing_pipeline,
                        channel_id=ch["id"],
                        channel_url=ch["channel_url"],
                        user_id=ch["user_id"],
                        language=ch.get("language", "hi")
                    )
        except Exception as e:
            log.error(f"Polling error: {e}")
        await asyncio.sleep(60)


# ══════════════════════════════════════════════════════
# CORE PIPELINE
# ══════════════════════════════════════════════════════
def run_indexing_pipeline(channel_id: str, channel_url: str, user_id: str,
                           language: str = "hi", max_videos: int = 50):
    """
    Full pipeline:
    1. Fetch video IDs from channel
    2. Download Hindi transcripts
    3. Save to Supabase DB + Storage
    4. Update channel status
    """
    log.info(f"🚀 Starting pipeline for: {channel_url}")

    # Update status to 'indexing'
    sb.table("channels").update({"status": "indexing"}).eq("id", channel_id).execute()

    try:
        # STEP 1: Get video IDs
        videos = get_video_ids(channel_url, max_videos)
        log.info(f"   Found {len(videos)} videos")

        if not videos:
            sb.table("channels").update({"status": "error", "error_message": "No videos found"}).eq("id", channel_id).execute()
            return

        # STEP 2: Download transcripts
        success, failed = 0, 0
        all_text = []

        for i, video in enumerate(videos):
            vid_id = video["id"]
            title  = video["title"]
            log.info(f"   [{i+1}/{len(videos)}] {title[:50]}")

            try:
                text = fetch_transcript(vid_id, language)
                if text:
                    cleaned = clean_transcript(text)
                    word_count = len(cleaned.split())

                    # STEP 3A: Save to Supabase DB (transcripts table)
                    save_transcript_to_db(
                        user_id=user_id,
                        channel_id=channel_id,
                        video_id=vid_id,
                        title=title,
                        content=cleaned,
                        word_count=word_count,
                        language=language
                    )

                    # STEP 3B: Save to Supabase Storage
                    save_transcript_to_storage(
                        user_id=user_id,
                        channel_id=channel_id,
                        video_id=vid_id,
                        title=title,
                        content=cleaned
                    )

                    all_text.append(cleaned)
                    success += 1
                    log.info(f"      ✅ {word_count:,} words saved")
                else:
                    failed += 1
                    log.info(f"      ⚠️  No transcript available")
            except Exception as e:
                failed += 1
                log.error(f"      ❌ Error: {e}")

            time.sleep(1.5)  # Rate limiting

        # STEP 4: Update channel status to 'indexed'
        total_words = sum(len(t.split()) for t in all_text)
        sb.table("channels").update({
            "status": "indexed",
            "videos_indexed": success,
            "words_indexed": total_words,
        }).eq("id", channel_id).execute()

        log.info(f"✅ Pipeline complete: {success} videos, {total_words:,} words")

    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
        sb.table("channels").update({
            "status": "error",
        }).eq("id", channel_id).execute()


# ══════════════════════════════════════════════════════
# TRANSCRIPT HELPERS
# ══════════════════════════════════════════════════════
def get_video_ids(channel_url: str, max_count: int = 50) -> list:
    """Fetch video IDs from a YouTube channel."""
    channel_url = channel_url.rstrip('/')
    url_variants = [
        channel_url + '/videos',
        channel_url,
        channel_url + '/streams',
    ]
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'playlistend': max_count,
        'ignoreerrors': True,
    }
    videos = []
    seen = set()
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for url in url_variants:
            try:
                info = ydl.extract_info(url, download=False)
                if not info:
                    continue
                for e in (info.get('entries') or []):
                    if not e:
                        continue
                    vid_id = e.get('id')
                    title  = e.get('title', 'Unknown')
                    if vid_id and vid_id not in seen and e.get('_type') != 'playlist':
                        seen.add(vid_id)
                        videos.append({'id': vid_id, 'title': title})
            except Exception:
                continue
            if videos:
                break
    return videos[:max_count]


def fetch_transcript(video_id: str, language: str = "hi") -> str | None:
    """Fetch transcript for a video in the given language."""
    ytt = YouTubeTranscriptApi()
    try:
        transcript_list = ytt.list(video_id)
        hindi_codes = ['hi', 'hi-IN', 'hi-Latn']

        # Try native transcript first
        for t in transcript_list:
            if t.language_code in hindi_codes:
                fetched = t.fetch()
                return ' '.join([s.text for s in fetched])

        # Fallback: translate auto-generated
        for t in transcript_list:
            if t.is_generated:
                try:
                    translated = t.translate('hi').fetch()
                    return ' '.join([s.text for s in translated])
                except Exception:
                    continue
    except Exception as e:
        log.error(f"Transcript fetch error for {video_id}: {e}")
    return None


def clean_transcript(text: str) -> str:
    """Clean transcript text."""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\[.*?\]', '', text)
    return text.strip()


def save_transcript_to_db(user_id, channel_id, video_id, title, content, word_count, language):
    """Save transcript as a row in the transcripts table."""
    try:
        sb.table("transcripts").upsert({
            "user_id":    user_id,
            "channel_id": channel_id,
            "video_id":   video_id,
            "title":      title,
            "content":    content,
            "word_count": word_count,
            "language":   language,
        }, on_conflict="video_id").execute()
    except Exception as e:
        log.error(f"DB save error: {e}")


def save_transcript_to_storage(user_id, channel_id, video_id, title, content):
    """Save transcript as a .txt file in Supabase Storage."""
    try:
        safe_title = re.sub(r'[^\w\s\-]', '', title)[:40].strip()
        file_path  = f"{user_id}/{channel_id}/{video_id}_{safe_title}.txt"
        file_content = f"Title: {title}\nVideo ID: {video_id}\nURL: https://youtube.com/watch?v={video_id}\n{'─'*60}\n\n{content}"

        sb.storage.from_("transcripts").upload(
            path=file_path,
            file=file_content.encode('utf-8'),
            file_options={"content-type": "text/plain", "upsert": "true"}
        )
    except Exception as e:
        log.error(f"Storage save error: {e}")
