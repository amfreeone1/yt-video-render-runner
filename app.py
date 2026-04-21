import os
import sys
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from pydantic import BaseModel, Field
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List
import json
import uuid
import time
import shutil
import logging
import re
import subprocess
import threading
import urllib.request
import urllib.parse
import tempfile

import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

_INSTANCE_ID = uuid.uuid4().hex[:8]
_BOOT_TIME = time.time()

app = FastAPI()
log = logging.getLogger("uvicorn.error")

BASE = Path(__file__).resolve().parent
JOBS = BASE / "jobs"
Q = JOBS / "queued"
P = JOBS / "processing"
D = JOBS / "done"
F = JOBS / "failed"
OUT = BASE / "outputs"
AUD = BASE / "audio"
QUOTA_LOCK_FILE = JOBS / "_youtube_quota_lock.json"

for d in [Q, P, D, F, OUT, AUD]:
    d.mkdir(parents=True, exist_ok=True)

REQUIRED_UPLOAD_ENV = [
    "RUNNER_SHARED_SECRET",
    "YOUTUBE_CLIENT_ID",
    "YOUTUBE_CLIENT_SECRET",
    "YOUTUBE_REFRESH_TOKEN",
]
_missing_upload_env = [k for k in REQUIRED_UPLOAD_ENV if not os.environ.get(k)]
if _missing_upload_env:
    print(f"FATAL: upload patch missing env vars: {_missing_upload_env}", file=sys.stderr)
    sys.exit(1)
print(f"✓ upload env vars present: {sorted(REQUIRED_UPLOAD_ENV)}")

log.info(f"Runner started instance_id={_INSTANCE_ID} active_jobs={len(list(Q.glob('*.json')) + list(P.glob('*.json')))}")

UPLOAD_KIND = "upload"
STALE_UPLOAD_THRESHOLD_SECONDS = 30 * 60
REAP_INTERVAL_SECONDS = 60.0
_LAST_REAP_AT = 0.0

_INTERNAL_TO_AP_STATUS = {
    "queued": "submitted",
    "processing": "uploading",
    "complete": "complete",
    "failed": "failed",
    "quota_exhausted": "quota_exhausted",
}

_YT_CLIENT = None


class RenderRequest(BaseModel):
    render_job_key: str
    audio_url: str
    content_id: str
    source_row_number: int


class VideoSource(BaseModel):
    source: str = "drive"
    drive_file_id: str
    url: Optional[str] = None


class UploadMetadata(BaseModel):
    title: str
    description: str = ""
    tags: List[str] = Field(default_factory=list)
    category_id: str
    default_language: str = "en"
    default_audio_language: str = "en"


class UploadPublish(BaseModel):
    privacy_status: str = "public"
    made_for_kids: bool = False
    publish_at: Optional[str] = None


class UploadJobRequest(BaseModel):
    upload_job_key: str
    content_id: str
    render_job_key: Optional[str] = ""
    video: VideoSource
    metadata: UploadMetadata
    publish: UploadPublish
    submitted_at: str


class QuotaExceededError(Exception):
    def __init__(self, quota_resets_at: str, message: str = "youtube daily upload quota reached"):
        super().__init__(message)
        self.quota_resets_at = quota_resets_at


class YTAuthError(Exception):
    pass


class YTValidationError(Exception):
    def __init__(self, reason: str, message: str):
        super().__init__(f"{reason}: {message}")
        self.reason = reason


class YTTransientError(Exception):
    pass


class YTUnknownError(Exception):
    pass


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def artifact_endpoint(key: str) -> str:
    return f"/render-jobs/{key}/artifact"


def job_path(folder: Path, key: str) -> Path:
    return folder / f"{key}.json"


def save_job(folder: Path, key: str, data: dict):
    job_path(folder, key).write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_job(folder: Path, key: str):
    path = job_path(folder, key)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def delete_job(folder: Path, key: str):
    path = job_path(folder, key)
    if path.exists():
        path.unlink()


def move_job(src: Path, dst: Path, key: str, data: dict):
    delete_job(src, key)
    save_job(dst, key, data)


def current_job(key: str):
    for folder, state in [(Q, "queued"), (P, "processing"), (D, "done"), (F, "failed")]:
        job = load_job(folder, key)
        if job:
            job["job_state_dir"] = state
            return job
    return None


def build_audio_download_url(url: str) -> str:
    m = re.search(r"/d/([A-Za-z0-9_-]+)", url or "")
    if m:
        file_id = m.group(1)
        return (
            f"https://drive.google.com/uc?export=download"
            f"&confirm=t&id={urllib.parse.quote(file_id)}"
        )
    return url


def validate_downloaded_audio(path: Path):
    """Raise RuntimeError with structured error tag if download result is bad."""
    if not path.exists():
        raise RuntimeError("downloaded_audio_missing")

    size = path.stat().st_size
    if size < 1024:
        raise RuntimeError("downloaded_audio_too_small")

    with path.open("rb") as f:
        head = f.read(512).lower()
    if b"<html" in head or b"<!doctype" in head:
        raise RuntimeError("downloaded_audio_is_html")

    ffprobe = shutil.which("ffprobe")
    if ffprobe:
        probe_cmd = [
            ffprobe, "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=codec_type",
            "-of", "csv=p=0",
            str(path),
        ]
        probe = subprocess.run(probe_cmd, capture_output=True, text=True, check=False)
        if probe.returncode != 0 or "audio" not in (probe.stdout or ""):
            raise RuntimeError("ffprobe_invalid_audio")


def download_file(url: str, dest: Path):
    req = urllib.request.Request(
        build_audio_download_url(url),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=180) as r, dest.open("wb") as f:
        shutil.copyfileobj(r, f)


def ap_status(internal_status: str) -> str:
    return _INTERNAL_TO_AP_STATUS.get(internal_status, internal_status)


def check_runner_auth(x_runner_auth: Optional[str]) -> None:
    expected = os.environ.get("RUNNER_SHARED_SECRET")
    if not expected:
        raise RuntimeError("RUNNER_SHARED_SECRET env var not set")
    if not x_runner_auth or x_runner_auth != expected:
        raise HTTPException(
            status_code=401,
            detail={"error_class": "AUTH", "error_message": "X-Runner-Auth invalid"},
        )


def safe_key_check(upload_job_key: str) -> None:
    if "/" in upload_job_key or ".." in upload_job_key or "\\" in upload_job_key:
        raise HTTPException(
            status_code=400,
            detail={"error_class": "VALIDATION", "error_message": "invalid characters in upload_job_key"},
        )


def read_quota_lock() -> Optional[str]:
    data = load_job(JOBS, "_youtube_quota_lock")
    return data.get("quota_locked_until") if data else None


def write_quota_lock(quota_resets_at: str):
    save_job(JOBS, "_youtube_quota_lock", {
        "quota_locked_until": quota_resets_at,
        "written_at": now(),
    })


def find_upload_job(upload_job_key: str):
    for folder in (Q, P, D, F):
        job = load_job(folder, upload_job_key)
        if job and job.get("kind") == UPLOAD_KIND:
            return folder, job
    return None


def get_youtube_client():
    global _YT_CLIENT
    if _YT_CLIENT is not None:
        return _YT_CLIENT
    creds = Credentials(
        token=None,
        refresh_token=os.environ["YOUTUBE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["YOUTUBE_CLIENT_ID"],
        client_secret=os.environ["YOUTUBE_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/youtube.upload"],
    )
    _YT_CLIENT = build("youtube", "v3", credentials=creds, cache_discovery=False)
    return _YT_CLIENT


def classify_youtube_error(e: HttpError):
    status_code = e.resp.status
    try:
        details = json.loads(e.content.decode("utf-8")).get("error", {})
        reasons = [err.get("reason", "") for err in details.get("errors", [])]
        message = details.get("message", str(e))[:500]
    except Exception:
        reasons = []
        message = str(e)[:500]

    if "quotaExceeded" in reasons or "uploadLimitExceeded" in reasons:
        quota_resets_at = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
        raise QuotaExceededError(quota_resets_at=quota_resets_at, message=message)
    if status_code in (401, 403) and any(r in reasons for r in ("authError", "forbidden", "insufficientPermissions")):
        raise YTAuthError(message)
    if 400 <= status_code < 500:
        reason = reasons[0] if reasons else "http_4xx"
        raise YTValidationError(reason=reason, message=message)
    if 500 <= status_code < 600:
        raise YTTransientError(message)
    raise YTUnknownError(f"http={status_code} message={message}")


def insert_video(video_path: str, metadata: dict, publish: dict) -> dict:
    client = get_youtube_client()
    body = {
        "snippet": {
            "title": metadata["title"],
            "description": metadata.get("description", ""),
            "tags": metadata.get("tags", []),
            "categoryId": metadata.get("category_id", "27"),
            "defaultLanguage": metadata.get("default_language", "en"),
            "defaultAudioLanguage": metadata.get("default_audio_language", "en"),
        },
        "status": {
            "privacyStatus": publish.get("privacy_status", "public"),
            "selfDeclaredMadeForKids": publish.get("made_for_kids", False),
        },
    }
    if publish.get("publish_at"):
        body["status"]["publishAt"] = publish["publish_at"]

    media = MediaFileUpload(video_path, chunksize=-1, resumable=True, mimetype="video/*")
    try:
        request = client.videos().insert(part="snippet,status", body=body, media_body=media)
        response = None
        while response is None:
            _, response = request.next_chunk()
    except HttpError as e:
        classify_youtube_error(e)
    except Exception as e:
        raise YTTransientError(str(e)[:500])

    video_id = response.get("id")
    if not video_id:
        raise YTUnknownError("YouTube response missing id field")
    return {
        "yt_video_id": video_id,
        "yt_video_url": f"https://www.youtube.com/watch?v={video_id}",
        "published_at": now(),
    }


def drive_fetch_via_service_account(drive_file_id: str) -> str:
    from google.oauth2 import service_account

    sa_json = os.environ["GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(sa_json) if sa_json.strip().startswith("{") else json.load(open(sa_json, encoding="utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    request = drive.files().get_media(fileId=drive_file_id)
    fd, path = tempfile.mkstemp(suffix=".mp4")
    os.close(fd)
    with open(path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request, chunksize=8 * 1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return path


def drive_fetch_via_public_link(drive_file_id: str) -> str:
    url = f"https://drive.google.com/uc?export=download&confirm=t&id={drive_file_id}"
    fd, path = tempfile.mkstemp(suffix=".mp4")
    os.close(fd)
    with requests.get(url, stream=True, allow_redirects=True, timeout=300) as r:
        r.raise_for_status()
        if "text/html" in r.headers.get("Content-Type", "").lower():
            raise RuntimeError("Drive returned HTML page; file may be too large or restricted")
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
    return path


def drive_fetch(drive_file_id: str) -> str:
    if os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON"):
        return drive_fetch_via_service_account(drive_file_id)
    return drive_fetch_via_public_link(drive_file_id)


def process_upload_job(key: str) -> None:
    job = load_job(P, key)
    if not job or job.get("kind") != UPLOAD_KIND:
        return

    if job["status"] not in ("queued", "processing"):
        return

    job["status"] = "processing"
    job["job_state_dir"] = "processing"
    job["started_at"] = now()
    job["updated_at"] = now()
    save_job(P, key, job)
    log.info("UPLOAD %s started", key)

    video_path = None
    try:
        quota_lock = read_quota_lock()
        if quota_lock:
            try:
                if datetime.fromisoformat(quota_lock.replace("Z", "+00:00")) > datetime.now(timezone.utc):
                    job["status"] = "quota_exhausted"
                    job["error_class"] = "QUOTA"
                    job["error_message"] = "youtube quota locked at processing time"
                    job["quota_resets_at"] = quota_lock
                    job["updated_at"] = now()
                    delete_job(P, key)
                    save_job(F, key, job)
                    log.info("UPLOAD %s quota_exhausted", key)
                    return
            except ValueError:
                pass

        try:
            video_path = drive_fetch(job["video"]["drive_file_id"])
        except Exception as e:
            job["status"] = "failed"
            job["error_class"] = "VALIDATION"
            job["error_message"] = f"drive_fetch_failed: {str(e)[:400]}"
            return

        try:
            result = insert_video(video_path, job["metadata"], job["publish"])
            job["status"] = "complete"
            job["job_state_dir"] = "done"
            job["progress_pct"] = 100
            job["yt_video_id"] = result["yt_video_id"]
            job["yt_video_url"] = result["yt_video_url"]
            job["published_at"] = result["published_at"]
            log.info("UPLOAD %s complete yt_video_id=%s", key, result["yt_video_id"])
        except QuotaExceededError as e:
            job["status"] = "quota_exhausted"
            job["error_class"] = "QUOTA"
            job["error_message"] = str(e)[:500]
            job["quota_resets_at"] = e.quota_resets_at
            write_quota_lock(e.quota_resets_at)
            log.info("UPLOAD %s quota_exhausted resets_at=%s", key, e.quota_resets_at)
        except YTAuthError as e:
            job["status"] = "failed"
            job["error_class"] = "AUTH"
            job["error_message"] = str(e)[:500]
        except YTValidationError as e:
            job["status"] = "failed"
            job["error_class"] = "HTTP_4XX"
            job["error_message"] = str(e)[:500]
        except YTTransientError:
            try:
                result = insert_video(video_path, job["metadata"], job["publish"])
                job["status"] = "complete"
                job["job_state_dir"] = "done"
                job["progress_pct"] = 100
                job["yt_video_id"] = result["yt_video_id"]
                job["yt_video_url"] = result["yt_video_url"]
                job["published_at"] = result["published_at"]
            except Exception as e2:
                job["status"] = "failed"
                job["error_class"] = "HTTP_5XX"
                job["error_message"] = str(e2)[:500]
        except Exception as e:
            job["status"] = "failed"
            job["error_class"] = "UNKNOWN"
            job["error_message"] = str(e)[:500]
    finally:
        job["updated_at"] = now()
        delete_job(P, key)
        if job["status"] == "complete":
            job["job_state_dir"] = "done"
            save_job(D, key, job)
        else:
            job["job_state_dir"] = "failed"
            save_job(F, key, job)
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except Exception:
                pass


def reap_stale_uploads() -> None:
    global _LAST_REAP_AT
    now_ts = time.time()
    if now_ts - _LAST_REAP_AT < REAP_INTERVAL_SECONDS:
        return
    _LAST_REAP_AT = now_ts

    if not P.exists():
        return

    threshold = datetime.now(timezone.utc) - timedelta(seconds=STALE_UPLOAD_THRESHOLD_SECONDS)
    for path in P.glob("*.json"):
        try:
            job = load_job(P, path.stem)
            if not job or job.get("kind") != UPLOAD_KIND:
                continue
            if job.get("status") != "processing":
                continue
            started_at_str = job.get("started_at")
            if not started_at_str:
                continue
            started_at = datetime.fromisoformat(started_at_str.replace("Z", "+00:00"))
            if started_at < threshold:
                key = path.stem
                job["status"] = "failed"
                job["job_state_dir"] = "failed"
                job["error_class"] = "UNKNOWN"
                job["error_message"] = f"worker_stalled (started_at={started_at_str})"
                job["updated_at"] = now()
                delete_job(P, key)
                save_job(F, key, job)
                log.info("UPLOAD %s reaped as stale", key)
        except Exception as e:
            log.exception("reap_stale_uploads error on %s: %s", path, e)


def process_job(key: str):
    job = load_job(P, key)
    if not job:
        return

    if job.get("kind") == UPLOAD_KIND:
        process_upload_job(key)
        return

    try:
        audio_file = AUD / f"{key}.mp3"
        output_file = OUT / f"{key}.mp4"

        job["status"] = "processing"
        job["job_state_dir"] = "processing"
        job["updated_at"] = now()
        save_job(P, key, job)

        log.info("JOB %s audio download start", key)
        download_file(job["audio_url"], audio_file)
        log.info(
            "JOB %s audio download complete bytes=%s",
            key,
            audio_file.stat().st_size if audio_file.exists() else 0,
        )

        validate_downloaded_audio(audio_file)
        log.info("JOB %s audio validation passed", key)

        cmd = [
            "ffmpeg",
            "-y",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=1080x1920:r=30",
            "-i",
            str(audio_file),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-shortest",
            str(output_file),
        ]
        log.info("JOB %s ffmpeg start output=%s", key, output_file.name)
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError("ffmpeg_failed")
        if not output_file.exists() or output_file.stat().st_size == 0:
            raise RuntimeError("output_artifact_missing")
        log.info("JOB %s ffmpeg complete bytes=%s", key, output_file.stat().st_size)

        job["status"] = "completed"
        job["job_state_dir"] = "done"
        job["video_url"] = ""
        job["output_file"] = output_file.name
        job["artifact_ready"] = True
        job["artifact_endpoint"] = artifact_endpoint(key)
        job["error_message"] = ""
        job["updated_at"] = now()
        delete_job(P, key)
        save_job(D, key, job)
        log.info("JOB %s completed", key)

    except Exception as e:
        log.exception("JOB %s processing failed", key)
        job = load_job(P, key) or job
        job["status"] = "failed"
        job["job_state_dir"] = "failed"
        job["video_url"] = ""
        job["output_file"] = ""
        job["artifact_ready"] = False
        job["artifact_endpoint"] = ""
        job["error_message"] = str(e)
        job["updated_at"] = now()
        delete_job(P, key)
        save_job(F, key, job)


def worker_loop():
    while True:
        try:
            reap_stale_uploads()
            for file in list(Q.glob("*.json")):
                key = file.stem
                job = load_job(Q, key)
                if not job:
                    continue

                job["status"] = "processing"
                job["job_state_dir"] = "processing"
                job["updated_at"] = now()
                move_job(Q, P, key, job)
                log.info("JOB %s accepted for background processing", key)
                threading.Thread(target=process_job, args=(key,), daemon=True).start()

        except Exception:
            log.exception("worker loop error")

        import time
        time.sleep(5)


threading.Thread(target=worker_loop, daemon=True).start()


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "yt-video-render-runner",
        "ffmpeg_ready": shutil.which("ffmpeg") is not None,
        "time": now(),
        "instance_id": _INSTANCE_ID,
        "uptime_seconds": int(time.time() - _BOOT_TIME),
        "active_jobs": len(list(Q.glob('*.json')) + list(P.glob('*.json'))),
    }


@app.post("/render-jobs")
def submit_job(req: RenderRequest):
    job = {
        "found": True,
        "render_job_key": req.render_job_key,
        "job_id": f"job_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}",
        "status": "queued",
        "job_state_dir": "queued",
        "source_row_number": req.source_row_number,
        "content_id": req.content_id,
        "audio_url": req.audio_url,
        "video_url": "",
        "output_file": "",
        "artifact_ready": False,
        "artifact_endpoint": "",
        "error_message": "",
        "received_at": now(),
        "updated_at": now(),
    }
    save_job(Q, req.render_job_key, job)
    log.info("JOB %s submitted", req.render_job_key)
    return JSONResponse(status_code=202, content=job)


@app.get("/render-jobs/{key}")
def get_job(key: str):
    job = current_job(key)
    if not job:
        return JSONResponse(status_code=404, content={"detail": "render job not found"})

    output_file = job.get("output_file", "")
    artifact_ready = bool(output_file) and (OUT / output_file).exists()
    return {
        "found": True,
        "render_job_key": key,
        "job_id": job.get("job_id", ""),
        "status": job.get("status", ""),
        "job_state_dir": job.get("job_state_dir", ""),
        "source_row_number": job.get("source_row_number"),
        "content_id": job.get("content_id", ""),
        "video_url": job.get("video_url", ""),
        "output_file": output_file,
        "artifact_ready": artifact_ready,
        "artifact_endpoint": artifact_endpoint(key) if artifact_ready else "",
        "error_message": job.get("error_message", ""),
        "received_at": job.get("received_at", ""),
        "updated_at": job.get("updated_at", job.get("received_at", "")),
    }


@app.get("/render-jobs/{key}/artifact")
def get_artifact(key: str):
    job = load_job(D, key)
    if not job:
        raise HTTPException(status_code=404, detail="render job not found")

    output_file = job.get("output_file", "")
    if not output_file:
        raise HTTPException(status_code=404, detail="output file missing")

    path = OUT / output_file
    if not path.exists():
        raise HTTPException(status_code=404, detail="artifact not found")

    return FileResponse(path, media_type="video/mp4", filename=output_file)


@app.get("/render-jobs/{key}/video")
def get_video(key: str):
    return get_artifact(key)


@app.post("/render-jobs/{key}/mark-failed")
def mark_failed(key: str):
    job = current_job(key)
    if not job:
        raise HTTPException(status_code=404, detail="render job not found")

    delete_job(Q, key)
    delete_job(P, key)
    delete_job(D, key)

    job["status"] = "failed"
    job["job_state_dir"] = "failed"
    job["error_message"] = "manual terminal failure test"
    job["updated_at"] = now()
    save_job(F, key, job)

    return {
        "ok": True,
        "render_job_key": key,
        "status": "failed",
        "updated_at": job["updated_at"],
    }


@app.post("/render-jobs/{key}/mark-complete")
def mark_complete(key: str):
    job = current_job(key)
    if not job:
        raise HTTPException(status_code=404, detail="render job not found")

    output_file = OUT / f"{key}.mp4"
    if not output_file.exists():
        raise HTTPException(status_code=409, detail="artifact not ready")

    delete_job(Q, key)
    delete_job(P, key)
    delete_job(F, key)

    job["status"] = "completed"
    job["job_state_dir"] = "done"
    job["video_url"] = ""
    job["output_file"] = output_file.name
    job["artifact_ready"] = True
    job["artifact_endpoint"] = artifact_endpoint(key)
    job["error_message"] = ""
    job["updated_at"] = now()
    save_job(D, key, job)

    return {
        "ok": True,
        "render_job_key": key,
        "status": "completed",
        "output_file": output_file.name,
        "artifact_ready": True,
        "artifact_endpoint": artifact_endpoint(key),
        "updated_at": job["updated_at"],
    }


@app.post("/upload-jobs", status_code=202)
def create_upload_job(
    body: UploadJobRequest,
    request: Request,
    x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    check_runner_auth(x_runner_auth)

    if not idempotency_key or idempotency_key != body.upload_job_key:
        raise HTTPException(
            status_code=400,
            detail={"error_class": "VALIDATION", "error_message": "Idempotency-Key header must match upload_job_key"},
        )
    safe_key_check(body.upload_job_key)
    if not body.video.drive_file_id:
        raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "video.drive_file_id is required"})
    if not body.metadata.title:
        raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "metadata.title is required"})
    if not body.metadata.category_id:
        raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "metadata.category_id is required"})

    explicit_base = os.environ.get("RUNNER_BASE_URL")
    base_url = explicit_base.rstrip("/") if explicit_base else f"{request.url.scheme}://{request.url.netloc}"
    poll_url = f"{base_url}/upload-jobs/{body.upload_job_key}"

    existing = find_upload_job(body.upload_job_key)
    if existing:
        _, job = existing
        return JSONResponse(status_code=200, content={
            "upload_job_key": job["upload_job_key"],
            "status": ap_status(job["status"]),
            "poll_url": poll_url,
            "submitted_at": job["submitted_at"],
        })

    quota_lock = read_quota_lock()
    now_dt = datetime.now(timezone.utc)
    if quota_lock:
        try:
            if datetime.fromisoformat(quota_lock.replace("Z", "+00:00")) > now_dt:
                job = {
                    **body.model_dump(),
                    "kind": UPLOAD_KIND,
                    "status": "quota_exhausted",
                    "job_state_dir": "failed",
                    "progress_pct": 0,
                    "yt_video_id": None,
                    "yt_video_url": None,
                    "published_at": None,
                    "error_class": "QUOTA",
                    "error_message": "youtube daily upload quota reached (pre-check)",
                    "quota_resets_at": quota_lock,
                    "started_at": None,
                    "received_at": now(),
                    "updated_at": now(),
                }
                save_job(F, body.upload_job_key, job)
                return {
                    "upload_job_key": job["upload_job_key"],
                    "status": ap_status(job["status"]),
                    "poll_url": poll_url,
                    "submitted_at": job["submitted_at"],
                }
        except ValueError:
            pass

    job = {
        **body.model_dump(),
        "kind": UPLOAD_KIND,
        "status": "queued",
        "job_state_dir": "queued",
        "progress_pct": 0,
        "yt_video_id": None,
        "yt_video_url": None,
        "published_at": None,
        "error_class": None,
        "error_message": None,
        "quota_resets_at": None,
        "started_at": None,
        "received_at": now(),
        "updated_at": now(),
    }
    save_job(Q, body.upload_job_key, job)
    return {
        "upload_job_key": job["upload_job_key"],
        "status": ap_status(job["status"]),
        "poll_url": poll_url,
        "submitted_at": job["submitted_at"],
    }


@app.get("/upload-jobs/{upload_job_key}")
def get_upload_job(
    upload_job_key: str,
    x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
):
    check_runner_auth(x_runner_auth)
    safe_key_check(upload_job_key)

    result = find_upload_job(upload_job_key)
    if not result:
        raise HTTPException(status_code=404, detail={"error_class": "NOT_FOUND", "error_message": "upload_job_key unknown"})
    _, job = result

    return {
        "upload_job_key": job["upload_job_key"],
        "status": ap_status(job.get("status", "")),
        "progress_pct": job.get("progress_pct", 0),
        "yt_video_id": job.get("yt_video_id"),
        "yt_video_url": job.get("yt_video_url"),
        "published_at": job.get("published_at"),
        "error_class": job.get("error_class"),
        "error_message": job.get("error_message"),
        "quota_resets_at": job.get("quota_resets_at"),
        "last_updated_at": job.get("updated_at", job.get("received_at", "")),
    }
