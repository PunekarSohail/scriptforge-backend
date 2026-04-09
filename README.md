# ScriptForge Backend

FastAPI backend that automatically indexes YouTube channel transcripts.

## Setup

### 1. Environment Variables (set in Railway dashboard)
```
SUPABASE_URL=https://pdezqdtfsukuokqpoyux.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key-here
```

### 2. Supabase — Add transcripts table
Run this SQL in Supabase SQL Editor:

```sql
create table transcripts (
  id uuid default gen_random_uuid() primary key,
  user_id uuid references profiles(id) on delete cascade,
  channel_id uuid references channels(id) on delete cascade,
  video_id text unique,
  title text,
  content text,
  word_count integer,
  language text default 'hi',
  created_at timestamp default now()
);

alter table transcripts enable row level security;

create policy "Users can view own transcripts"
  on transcripts for select using (auth.uid() = user_id);

create policy "Service can insert transcripts"
  on transcripts for insert with check (true);

create policy "Service can update transcripts"
  on transcripts for update using (true);
```

### 3. Supabase Storage — Create bucket
1. Go to Supabase → Storage
2. Create a new bucket called: `transcripts`
3. Set it to Private

### 4. Supabase Webhook — Auto-trigger indexing
1. Go to Supabase → Database → Webhooks
2. Click "Create a new hook"
3. Name: `channel-added`
4. Table: `channels`
5. Events: `INSERT`
6. URL: `https://your-railway-url.railway.app/webhook/channel-added`

### 5. Add language column to channels table
```sql
alter table channels add column if not exists language text default 'hi';
alter table channels add column if not exists error_message text;
```

## API Endpoints

- `GET /` — Health check
- `GET /health` — Detailed health
- `POST /webhook/channel-added` — Supabase webhook trigger
- `POST /index-channel` — Manual trigger
- `GET /channel-status/{channel_id}` — Check status

## Deploy to Railway

1. Push this folder to a GitHub repo
2. Go to railway.app → New Project → Deploy from GitHub
3. Select the repo
4. Add environment variables in Railway dashboard
5. Deploy!
