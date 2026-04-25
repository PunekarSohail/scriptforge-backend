"""
ScriptForge Backend — FastAPI Server
Runs on Railway.app
Handles: channel indexing, transcript storage, webhook from Supabase
"""

import os
import re
import time
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
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

# ── SUPABASE ──────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://pdezqdtfsukuokqpoyux.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
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
    channel_id: str
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
# MANUAL TRIGGER
# ══════════════════════════════════════════════════════
@app.post("/index-channel")
async def index_channel_manual(req: IndexChannelRequest, background_tasks: BackgroundTasks):
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
    result = sb.table("channels").select("*").eq("id", channel_id).single().execute()
    if result.data:
        return result.data
    raise HTTPException(status_code=404, detail="Channel not found")

# ══════════════════════════════════════════════════════
# POLLING
# ══════════════════════════════════════════════════════
@app.on_event("startup")
async def start_polling():
    asyncio.create_task(poll_pending_channels())

async def poll_pending_channels():
    log.info("🔄 Polling started for pending channels...")
    while True:
        try:
            result  = sb.table("channels").select("*").eq("status", "processing").execute()
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
    log.info(f"🚀 Starting pipeline for: {channel_url}")
    sb.table("channels").update({"status": "indexing"}).eq("id", channel_id).execute()

    try:
        videos = get_video_ids(channel_url, max_videos)
        log.info(f"   Found {len(videos)} videos")

        if not videos:
            sb.table("channels").update({"status": "error", "error_message": "No videos found"}).eq("id", channel_id).execute()
            return

        success, failed = 0, 0
        all_text = []

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
                    failed += 1
                    log.info(f"      ⚠️  No transcript available")
            except Exception as e:
                failed += 1
                log.error(f"      ❌ Error: {e}")

            time.sleep(1.5)

        total_words = sum(len(t.split()) for t in all_text)
        sb.table("channels").update({
            "status":         "indexed",
            "videos_indexed": success,
            "words_indexed":  total_words,
        }).eq("id", channel_id).execute()

        log.info(f"✅ Pipeline complete: {success} videos, {total_words:,} words")

    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
        sb.table("channels").update({"status": "error"}).eq("id", channel_id).execute()

# ══════════════════════════════════════════════════════
# YOUTUBE DATA API v3 — VIDEO FETCHING
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

    log.warning(f"Could not resolve channel ID from URL: {channel_url}")
    return None


def get_video_ids(channel_url: str, max_count: int = 5) -> list:
    """Fetch video IDs using YouTube Data API v3 — no IP ban."""
    youtube    = build_youtube()
    channel_id = _resolve_channel_id(youtube, channel_url)
    if not channel_id:
        log.error(f"Cannot resolve channel ID for: {channel_url}")
        return []

    ch_res   = youtube.channels().list(part="contentDetails,snippet", id=channel_id).execute()
    ch_items = ch_res.get("items", [])
    if not ch_items:
        log.error(f"Channel not found: {channel_id}")
        return []

    uploads_playlist_id = (
        ch_items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
    )
    if not uploads_playlist_id:
        log.error(f"No uploads playlist for: {channel_id}")
        return []

    log.info(f"   Uploads playlist: {uploads_playlist_id}")
    videos          = []
    next_page_token = None

    while len(videos) < max_count:
        batch_size = min(50, max_count - len(videos))
        res = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=batch_size,
            pageToken=next_page_token
        ).execute()

        for item in res.get("items", []):
            snippet  = item.get("snippet", {})
            resource = snippet.get("resourceId", {})
            vid_id   = resource.get("videoId")
            title    = snippet.get("title", "Unknown")
            if vid_id:
                videos.append({"id": vid_id, "title": title})

        next_page_token = res.get("nextPageToken")
        if not next_page_token:
            break

    log.info(f"   Fetched {len(videos)} videos via YouTube Data API v3")
    return videos[:max_count]


# ══════════════════════════════════════════════════════
# TRANSCRIPT FETCHING — cookies-based to bypass IP ban
# ══════════════════════════════════════════════════════
def fetch_transcript(video_id: str, language: str = "hi") -> str | None:
    """
    Fetch transcript using youtube-transcript-api with YouTube cookies.
    Cookies bypass Railway's cloud IP ban on transcript fetching.
    Upload cookies.txt to the repo root to enable this.
    """
    hindi_codes  = ['hi', 'hi-IN', 'hi-Latn']
    cookies_file = Path(__file__).parent / "cookies.txt"

    try:
        if cookies_file.exists():
            log.info(f"      Using cookies.txt for auth")
            ytt = YouTubeTranscriptApi(cookie_path=str(cookies_file))
        else:
            log.warning(f"      No cookies.txt found — trying without auth (may be blocked)")
            ytt = YouTubeTranscriptApi()

        transcript_list = ytt.list(video_id)

        # Try native Hindi first
        for t in transcript_list:
            if t.language_code in hindi_codes:
                fetched = t.fetch()
                return ' '.join([s.text for s in fetched])

        # Fallback: translate auto-generated to Hindi
        for t in transcript_list:
            if t.is_generated:
                try:
                    translated = t.translate('hi').fetch()
                    return ' '.join([s.text for s in translated])
                except Exception:
                    continue

    except Exception as e:
        log.error(f"      Transcript fetch error for {video_id}: {e}")

    return None


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
    try:
        safe_title   = re.sub(r'[^\w\s\-]', '', title)[:40].strip()
        file_path    = f"{user_id}/{channel_id}/{video_id}_{safe_title}.txt"
        file_content = f"Title: {title}\nVideo ID: {video_id}\nURL: https://youtube.com/watch?v={video_id}\n{'─'*60}\n\n{content}"
        sb.storage.from_("transcripts").upload(
            path=file_path,
            file=file_content.encode('utf-8'),
            file_options={"content-type": "text/plain", "upsert": "true"}
        )
    except Exception as e:
        log.error(f"Storage save error: {e}")
