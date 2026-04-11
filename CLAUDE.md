# CLAUDE.md — yt-video-render-runner

## What this is

Self-hosted FastAPI video render service. Accepts an audio URL,
produces a 1080×1920 black-background MP4 via ffmpeg.
Backend worker for a YouTube Shorts automation pipeline.

## Architecture

```
POST /render-jobs
        │
        ▼
  jobs/queued/{key}.json
        │  (worker_loop polls every 5 s)
        ▼
  jobs/processing/{key}.json
        │  (daemon thread per job)
        │
        ├─► download audio  →  audio/{key}.mp3
        ├─► ffmpeg render   →  outputs/{key}.mp4
        │
        ├─ success ──► jobs/done/{key}.json
        └─ failure ──► jobs/failed/{key}.json
```

## Key directories

| Path | Purpose |
|------|---------|
| `audio/` | Downloaded audio files (mp3) |
| `outputs/` | Rendered video artifacts (mp4) |
| `jobs/queued/` | Submitted, awaiting pickup |
| `jobs/processing/` | Currently rendering |
| `jobs/done/` | Completed successfully |
| `jobs/failed/` | Terminal failures |

## API surface

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness check + ffmpeg availability |
| `POST` | `/render-jobs` | Submit a new render job |
| `GET` | `/render-jobs/{key}` | Poll job status |
| `GET` | `/render-jobs/{key}/artifact` | Download finished MP4 |
| `GET` | `/render-jobs/{key}/video` | Alias for `/artifact` |
| `POST` | `/render-jobs/{key}/mark-failed` | Force-fail (debug) |
| `POST` | `/render-jobs/{key}/mark-complete` | Force-complete (debug) |

## Deployment

- **Host:** Render.com (deploy hook in `.github/workflows/`)
- **Runtime:** Python 3.x + ffmpeg on `$PATH`
- **Start:** `pip install -r requirements.txt && uvicorn app:app --host 0.0.0.0 --port $PORT`

## Job contract (canonical fields)

```json
{
  "found": true,
  "render_job_key": "...",
  "job_id": "job_YYYYMMDD_HHMMSS_xxxxxxxx",
  "status": "queued | processing | completed | failed",
  "source_row_number": 3,
  "content_id": "...",
  "audio_url": "...",
  "video_url": "",
  "output_file": "{key}.mp4",
  "artifact_ready": true,
  "artifact_endpoint": "/render-jobs/{key}/artifact",
  "error_message": "",
  "received_at": "ISO8601Z",
  "updated_at": "ISO8601Z"
}
```

## Known risks (audit 2026-04-11)

1. **Google Drive large-file download** — `uc?export=download` may return
   HTML warning page instead of audio for files >100 MB.
2. **No content validation** — downloaded file not checked for type/size
   before ffmpeg ingestion.
3. **No retry** — single download attempt, 180 s timeout.
4. **Unbounded concurrency** — one thread per job, no pool cap.
5. **File-system queue** — no locking; safe only in single-process mode.
6. **`video_url` dead field** — always empty, never populated.

## Error taxonomy (target)

| `error_message` value | Meaning |
|----------------------|---------|
| `downloaded_audio_missing` | File did not appear on disk after download |
| `downloaded_audio_too_small` | File < 1 KB — likely error page |
| `downloaded_audio_is_html` | Magic bytes show HTML, not media |
| `ffprobe_invalid_audio` | ffprobe could not detect a valid audio stream |
| `ffmpeg_failed` | ffmpeg render returned non-zero exit |
| `output_artifact_missing` | MP4 not on disk after ffmpeg |

## PR discipline

- Never commit directly to `main`.
- All changes go through a feature branch + PR.
- Minimal scope per PR — one concern at a time.
