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

from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from supabase import create_client, Client
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── LOGGING ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# ── YOUTUBE DATA API v3 ───────────────────────────────
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")

def build_youtube():
    """Build and return an authenticated YouTube Data API v3 service object."""
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

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
    max_videos: int = 5

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
                           language: str = "hi", max_videos: int = 5):
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
# YOUTUBE DATA API v3 HELPERS
# ══════════════════════════════════════════════════════
def _resolve_channel_id(youtube, channel_url: str) -> str | None:
    """
    Resolve a channel URL string to a YouTube channel ID.
    Supports:
      - https://www.youtube.com/channel/UC...
      - https://www.youtube.com/@handle
      - https://www.youtube.com/c/CustomName
      - https://www.youtube.com/user/Username
    """
    channel_url = channel_url.rstrip('/')

    # Direct channel ID in URL
    match = re.search(r'/channel/(UC[\w-]+)', channel_url)
    if match:
        return match.group(1)

    # Handle-based URL (@handle) — use forHandle search
    handle_match = re.search(r'/@([\w.-]+)', channel_url)
    if handle_match:
        handle = handle_match.group(1)
        res = youtube.channels().list(
            part="id",
            forHandle=handle
        ).execute()
        items = res.get("items", [])
        if items:
            return items[0]["id"]

    # /c/ or /user/ custom name — use search fallback
    custom_match = re.search(r'/(?:c|user)/([\w.-]+)', channel_url)
    if custom_match:
        name = custom_match.group(1)
        res = youtube.channels().list(
            part="id",
            forUsername=name
        ).execute()
        items = res.get("items", [])
        if items:
            return items[0]["id"]

    log.warning(f"Could not resolve channel ID from URL: {channel_url}")
    return None


def get_video_ids(channel_url: str, max_count: int = 50) -> list:
    """
    Fetch up to max_count video IDs from a YouTube channel
    using the YouTube Data API v3 (uploads playlist).
    """
    youtube = build_youtube()

    # Step 1: Resolve channel ID
    channel_id = _resolve_channel_id(youtube, channel_url)
    if not channel_id:
        log.error(f"Cannot resolve channel ID for: {channel_url}")
        return []

    # Step 2: Get the uploads playlist ID from channel details
    ch_res = youtube.channels().list(
        part="contentDetails,snippet",
        id=channel_id
    ).execute()
    ch_items = ch_res.get("items", [])
    if not ch_items:
        log.error(f"Channel not found in API: {channel_id}")
        return []

    uploads_playlist_id = (
        ch_items[0]
        .get("contentDetails", {})
        .get("relatedPlaylists", {})
        .get("uploads")
    )
    if not uploads_playlist_id:
        log.error(f"No uploads playlist for channel: {channel_id}")
        return []

    log.info(f"   Uploads playlist: {uploads_playlist_id}")

    # Step 3: Page through the uploads playlist
    videos = []
    next_page_token = None

    while len(videos) < max_count:
        batch_size = min(50, max_count - len(videos))  # API max per page = 50
        req = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=batch_size,
            pageToken=next_page_token
        )
        res = req.execute()

        for item in res.get("items", []):
            snippet = item.get("snippet", {})
            resource = snippet.get("resourceId", {})
            vid_id = resource.get("videoId")
            title  = snippet.get("title", "Unknown")
            if vid_id:
                videos.append({"id": vid_id, "title": title})

        next_page_token = res.get("nextPageToken")
        if not next_page_token:
            break

    log.info(f"   Fetched {len(videos)} videos via YouTube Data API v3")
    return videos[:max_count]


def fetch_transcript(video_id: str, language: str = "hi") -> str | None:
    """Fetch captions using YouTube Data API v3 — no IP ban."""
    youtube = build_youtube()
    try:
        # List available captions for the video
        captions_res = youtube.captions().list(
            part="snippet",
            videoId=video_id
        ).execute()

        caption_items = captions_res.get("items", [])
        if not caption_items:
            log.info(f"      No captions available for {video_id}")
            return None

        # Log available languages
        available = [(c["snippet"]["language"], c["id"]) for c in caption_items]
        log.info(f"      Available captions: {available}")

        # Find Hindi caption
        hindi_codes = ["hi", "hi-IN", "hi-Latn"]
        caption_id = None
        for lang_code, cap_id in available:
            if lang_code in hindi_codes:
                caption_id = cap_id
                break

        # Fallback to auto-generated (asr) if no Hindi
        if not caption_id:
            for item in caption_items:
                if item["snippet"].get("trackKind") == "asr":
                    caption_id = item["id"]
                    break

        # Fallback to first available
        if not caption_id and caption_items:
            caption_id = caption_items[0]["id"]

        if not caption_id:
            return None

        # Download the caption track
        caption_data = youtube.captions().download(
            id=caption_id,
            tfmt="srt"
        ).execute()

        # Parse SRT format — extract just the text
        text = re.sub(r'\d+\n\d{2}:\d{2}:\d{2},\d+ --> \d{2}:\d{2}:\d{2},\d+\n', '', caption_data.decode('utf-8'))
        text = re.sub(r'\n+', ' ', text)
        return text.strip()

    except Exception as e:
        log.error(f"Caption fetch error for {video_id}: {e}")
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
