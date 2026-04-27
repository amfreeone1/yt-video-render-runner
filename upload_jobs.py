"""
Upload jobs module — paired with app.py (file-based queue runner).

Adds POST /upload-jobs and GET /upload-jobs/{upload_job_key}.
Reuses the existing jobs/{queued,processing,done,failed} file-based queue.
Upload jobs are written into jobs/queued/{upload_job_key}.json with kind="upload",
get picked up by the existing worker_loop, dispatched via process_job's kind
branch to process_upload_job() defined here.

NO K/V store. NO Redis. NO Celery. Pure file-based, matching app.py idiom.
"""

import os
import json
import time
import tempfile
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Callable
from fastapi import FastAPI, HTTPException, Header, Request
from pydantic import BaseModel, Field

import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

# ---------- Constants ----------

UPLOAD_KIND = "upload"

BASE = Path(__file__).resolve().parent
JOBS = BASE / "jobs"
Q = JOBS / "queued"
P = JOBS / "processing"
D = JOBS / "done"
F = JOBS / "failed"
QUOTA_LOCK_FILE = JOBS / "_youtube_quota_lock.json"

STALE_UPLOAD_THRESHOLD_SECONDS = 30 * 60
_REAP_INTERVAL_SECONDS = 60.0
_last_reap_at = 0.0

# Internal-status -> AP-facing status (frozen Path 2 contract)
_INTERNAL_TO_AP_STATUS = {
    "queued": "submitted",
    "processing": "uploading",
    "complete": "complete",
    "failed": "failed",
    "quota_exhausted": "quota_exhausted",
}

def _ap_status(internal_status: str) -> str:
    return _INTERNAL_TO_AP_STATUS.get(internal_status, internal_status)

# ---------- Models ----------

class VideoSource(BaseModel):
    source: str = "drive"
    drive_file_id: str = ""
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

# ---------- Typed exceptions ----------

class QuotaExceededError(Exception):
    def __init__(self, quota_resets_at: str, message: str = "youtube daily upload quota reached"):
        super().__init__(message)
        self.quota_resets_at = quota_resets_at

class YTAuthError(Exception): pass

class YTValidationError(Exception):
    def __init__(self, reason: str, message: str):
        super().__init__(f"{reason}: {message}")
        self.reason = reason

class YTTransientError(Exception): pass
class YTUnknownError(Exception): pass

# ---------- Helpers ----------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _check_auth(x_runner_auth: Optional[str]) -> None:
    expected = os.environ.get("RUNNER_AUTH_TOKEN")
    if not expected:
        raise RuntimeError("RUNNER_AUTH_TOKEN env var not set")
    if not x_runner_auth or x_runner_auth != expected:
        raise HTTPException(
            status_code=401,
            detail={"error_class": "AUTH", "error_message": "X-Runner-Auth invalid"},
        )

def _safe_key_check(upload_job_key: str) -> None:
    if "/" in upload_job_key or ".." in upload_job_key or "\\" in upload_job_key:
        raise HTTPException(
            status_code=400,
            detail={"error_class": "VALIDATION", "error_message": "invalid characters in upload_job_key"},
        )

def _atomic_write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def _read_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _find_upload_job(upload_job_key: str) -> Optional[tuple]:
    """Return (folder, job_dict) for the upload job in any of Q/P/D/F. None if not found."""
    for folder in (Q, P, D, F):
        path = folder / f"{upload_job_key}.json"
        job = _read_json(path)
        if job and job.get("kind") == UPLOAD_KIND:
            return (folder, job)
    return None

def _read_quota_lock() -> Optional[str]:
    data = _read_json(QUOTA_LOCK_FILE)
    return data.get("quota_locked_until") if data else None

def _write_quota_lock(quota_resets_at: str) -> None:
    QUOTA_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(QUOTA_LOCK_FILE, {
        "quota_locked_until": quota_resets_at,
        "written_at": _now_iso(),
    })

def _build_job_state(body: UploadJobRequest, status_str: str) -> dict:
    return {
        "kind": UPLOAD_KIND,
        "upload_job_key": body.upload_job_key,
        "content_id": body.content_id,
        "render_job_key": body.render_job_key or "",
        "video": body.video.model_dump(),
        "metadata": body.metadata.model_dump(),
        "publish": body.publish.model_dump(),
        "status": status_str,
        "progress_pct": 0,
        "yt_video_id": None,
        "yt_video_url": None,
        "published_at": None,
        "error_class": None,
        "error_message": None,
        "quota_resets_at": None,
        "started_at": None,
        "submitted_at": body.submitted_at,
        "received_at": _now_iso(),
        "updated_at": _now_iso(),
    }

# ---------- YouTube client ----------

_YT_CLIENT = None

def _get_youtube_client():
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

def _classify_youtube_error(e: HttpError):
    status_code = e.resp.status
    try:
        details = json.loads(e.content.decode("utf-8")).get("error", {})
        reasons = [err.get("reason", "") for err in details.get("errors", [])]
        message = details.get("message", str(e))[:500]
    except Exception:
        reasons = []
        message = str(e)[:500]

    if "quotaExceeded" in reasons or "uploadLimitExceeded" in reasons:
        quota_resets_at = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat().replace("+00:00", "Z")
        raise QuotaExceededError(quota_resets_at=quota_resets_at, message=message)
    if status_code in (401, 403) and any(r in reasons for r in ("authError", "forbidden", "insufficientPermissions")):
        raise YTAuthError(message)
    if 400 <= status_code < 500:
        reason = reasons[0] if reasons else "http_4xx"
        raise YTValidationError(reason=reason, message=message)
    if 500 <= status_code < 600:
        raise YTTransientError(message)
    raise YTUnknownError(f"http={status_code} message={message}")

def _insert_video(video_path: str, metadata: dict, publish: dict) -> dict:
    client = _get_youtube_client()
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
        _classify_youtube_error(e)
    except Exception as e:
        raise YTTransientError(str(e)[:500])

    video_id = response.get("id")
    if not video_id:
        raise YTUnknownError("YouTube response missing id field")
    return {
        "yt_video_id": video_id,
        "yt_video_url": f"https://www.youtube.com/watch?v={video_id}",
        "published_at": _now_iso(),
    }

# ---------- Drive fetch ----------

def _drive_fetch_via_service_account(drive_file_id: str) -> str:
    from google.oauth2 import service_account
    sa_json = os.environ["GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(sa_json) if sa_json.strip().startswith("{") else json.load(open(sa_json))
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

def _drive_fetch_via_public_link(drive_file_id: str) -> str:
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

def _drive_fetch(drive_file_id: str) -> str:
    if os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON"):
        return _drive_fetch_via_service_account(drive_file_id)
    return _drive_fetch_via_public_link(drive_file_id)

# ---------- Worker entry point (invoked from app.py process_job dispatch) ----------

def process_upload_job(
    key: str,
    *,
    P: Path,
    D: Path,
    F: Path,
    save_job: Callable,
    delete_job: Callable,
    load_job: Callable,
    now: Callable,
    log,
) -> None:
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
        quota_lock = _read_quota_lock()
        if quota_lock:
            try:
                if datetime.fromisoformat(quota_lock.replace("Z", "+00:00")) > datetime.now(timezone.utc):
                    job["status"] = "quota_exhausted"
                    job["error_class"] = "QUOTA"
                    job["error_message"] = "youtube quota locked at processing time"
                    job["quota_resets_at"] = quota_lock
                    return
            except ValueError:
                pass

        try:
            video_source = job.get("video", {})
            if video_source.get("source") == "url" and video_source.get("url"):
                import requests as _req
                import tempfile as _tmp
                fd, video_path = _tmp.mkstemp(suffix=".mp4")
                os.close(fd)
                with _req.get(video_source["url"], stream=True, timeout=300, allow_redirects=True) as _r:
                    _r.raise_for_status()
                    with open(video_path, "wb") as _f:
                        for _chunk in _r.iter_content(chunk_size=8 * 1024 * 1024):
                            if _chunk:
                                _f.write(_chunk)
            else:
                video_path = _drive_fetch(job["video"]["drive_file_id"])
        except Exception as e:
            job["status"] = "failed"
            job["error_class"] = "VALIDATION"
            job["error_message"] = f"video_fetch_failed: {str(e)[:400]}"
            return

        try:
            result = _insert_video(video_path, job["metadata"], job["publish"])
            job["status"] = "complete"
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
            _write_quota_lock(e.quota_resets_at)
            log.info("UPLOAD %s quota_exhausted resets_at=%s", key, e.quota_resets_at)
        except YTAuthError as e:
            job["status"] = "failed"
            job["error_class"] = "AUTH"
            job["error_message"] = str(e)[:500]
        except YTValidationError as e:
            job["status"] = "failed"
            job["error_class"] = "HTTP_4XX"
            job["error_message"] = str(e)[:500]
        except YTTransientError as e:
            try:
                result = _insert_video(video_path, job["metadata"], job["publish"])
                job["status"] = "complete"
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

# ---------- Stale-upload reaper ----------

def reap_stale_uploads(
    *,
    P: Path,
    F: Path,
    save_job: Callable,
    delete_job: Callable,
    load_job: Callable,
    now: Callable,
    log,
) -> None:
    global _last_reap_at
    now_ts = time.time()
    if now_ts - _last_reap_at < _REAP_INTERVAL_SECONDS:
        return
    _last_reap_at = now_ts

    if not P.exists():
        return

    threshold = datetime.now(timezone.utc) - timedelta(seconds=STALE_UPLOAD_THRESHOLD_SECONDS)
    for path in P.glob("*.json"):
        try:
            job = _read_json(path)
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

# ---------- Route registration ----------

def register_upload_routes(app: FastAPI) -> None:

    @app.post("/upload-jobs", status_code=202)
    def create_upload_job(
        body: UploadJobRequest,
        request: Request,
        x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ):
        _check_auth(x_runner_auth)

        for _env_key in ["YOUTUBE_CLIENT_ID", "YOUTUBE_CLIENT_SECRET", "YOUTUBE_REFRESH_TOKEN"]:
            if not os.environ.get(_env_key):
                raise HTTPException(
                    status_code=503,
                    detail={"error_class": "CONFIG", "error_message": f"missing env var: {_env_key}"},
                )

        if not idempotency_key or idempotency_key != body.upload_job_key:
            raise HTTPException(
                status_code=400,
                detail={"error_class": "VALIDATION", "error_message": "Idempotency-Key header must match upload_job_key"},
            )
        _safe_key_check(body.upload_job_key)
        if body.video.source == "drive" and not body.video.drive_file_id:
            raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "video.drive_file_id is required when source=drive"})
        if not body.metadata.title:
            raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "metadata.title is required"})
        if not body.metadata.category_id:
            raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "metadata.category_id is required"})

        explicit_base = os.environ.get("RUNNER_BASE_URL")
        base_url = explicit_base.rstrip("/") if explicit_base else f"{request.url.scheme}://{request.url.netloc}"
        poll_url = f"{base_url}/upload-jobs/{body.upload_job_key}"

        existing = _find_upload_job(body.upload_job_key)
        if existing:
            _, job = existing
            return {
                "upload_job_key": job["upload_job_key"],
                "status": _ap_status(job["status"]),
                "poll_url": poll_url,
                "submitted_at": job["submitted_at"],
            }

        quota_lock = _read_quota_lock()
        now_dt = datetime.now(timezone.utc)
        if quota_lock:
            try:
                if datetime.fromisoformat(quota_lock.replace("Z", "+00:00")) > now_dt:
                    job = _build_job_state(body, status_str="quota_exhausted")
                    job["job_state_dir"] = "failed"
                    job["error_class"] = "QUOTA"
                    job["error_message"] = "youtube daily upload quota reached (pre-check)"
                    job["quota_resets_at"] = quota_lock
                    F.mkdir(parents=True, exist_ok=True)
                    _atomic_write_json(F / f"{body.upload_job_key}.json", job)
                    return {
                        "upload_job_key": job["upload_job_key"],
                        "status": _ap_status(job["status"]),
                        "poll_url": poll_url,
                        "submitted_at": job["submitted_at"],
                    }
            except ValueError:
                pass

        job = _build_job_state(body, status_str="queued")
        job["job_state_dir"] = "queued"
        Q.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(Q / f"{body.upload_job_key}.json", job)

        return {
            "upload_job_key": job["upload_job_key"],
            "status": _ap_status(job["status"]),
            "poll_url": poll_url,
            "submitted_at": job["submitted_at"],
        }

    @app.get("/upload-jobs/{upload_job_key}")
    def get_upload_job(
        upload_job_key: str,
        x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
    ):
        _check_auth(x_runner_auth)
        _safe_key_check(upload_job_key)

        result = _find_upload_job(upload_job_key)
        if not result:
            raise HTTPException(status_code=404, detail={"error_class": "NOT_FOUND", "error_message": "upload_job_key unknown"})
        _, job = result

        return {
            "upload_job_key": job["upload_job_key"],
            "status": _ap_status(job.get("status", "")),
            "progress_pct": job.get("progress_pct", 0),
            "yt_video_id": job.get("yt_video_id"),
            "yt_video_url": job.get("yt_video_url"),
            "published_at": job.get("published_at"),
            "error_class": job.get("error_class"),
            "error_message": job.get("error_message"),
            "quota_resets_at": job.get("quota_resets_at"),
            "last_updated_at": job.get("updated_at", job.get("received_at", "")),
        }
