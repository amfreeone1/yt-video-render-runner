import os
import asyncio
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from pydantic import BaseModel
from pathlib import Path
from datetime import datetime, timezone
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
import base64
import binascii
from io import BytesIO

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

from assembly_jobs import (
    register_assembly_routes,
    startup_scan_recover_interrupted,
    ASSEMBLE_PROCESSING,
)
from upload_jobs import (
    UPLOAD_KIND,
    process_upload_job,
    reap_stale_uploads,
    register_upload_routes,
)
from utils.drive_upload import upload_file_to_drive

_INSTANCE_ID = uuid.uuid4().hex[:8]
_BOOT_TIME = time.time()

app = FastAPI()
log = logging.getLogger("uvicorn.error")

MCP_SERVER_NAME = "hafis-drive-raw-upload-mcp"
MCP_SERVER_VERSION = "0.1.0"
MCP_PROTOCOL_VERSION = "2024-11-05"
MCP_TOOL_NAME = "upload_image_to_drive_and_share"
DEFAULT_MCP_IMAGE_FILENAME = "hafiz_ai_concept_instagram.jpg"
GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.file"


@app.middleware("http")
async def require_x_runner_auth(request: Request, call_next):
    if request.url.path.startswith(("/render-jobs", "/upload-jobs")):
        expected = os.environ.get("RUNNER_AUTH_TOKEN")
        supplied = request.headers.get("X-Runner-Auth")
        if not expected or supplied != expected:
            return JSONResponse(
                status_code=401,
                content={"error_class": "AUTH", "error_message": "Unauthorized"},
            )
    return await call_next(request)


BASE = Path(__file__).resolve().parent
JOBS = BASE / "jobs"
Q = JOBS / "queued"
P = JOBS / "processing"
D = JOBS / "done"
F = JOBS / "failed"
OUT = BASE / "outputs"
AUD = BASE / "audio"

for d in [Q, P, D, F, OUT, AUD]:
    d.mkdir(parents=True, exist_ok=True)

log.info(f"Runner started instance_id={_INSTANCE_ID} active_jobs={len(list(Q.glob('*.json')) + list(P.glob('*.json')))}")


class RenderRequest(BaseModel):
    render_job_key: str
    audio_url: str
    content_id: str
    source_row_number: int


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


_ASSEMBLY_HEALTH_LOCK = threading.Lock()
_ASSEMBLY_HEALTH_SUMMARY_DEFAULT = {
    "scan_state": "not_started",
    "persisted_non_terminal_count": 0,
    "known_unreconciled_count": 0,
    "known_interrupted_count": 0,
    "local_processing_count": 0,
    "reconciled": True,
    "last_scan_started_at": None,
    "last_scan_completed_at": None,
    "last_scan_error": "",
    "last_scan_summary": None,
}
_ASSEMBLY_HEALTH_SUMMARY = dict(_ASSEMBLY_HEALTH_SUMMARY_DEFAULT)


def _reset_assembly_health_summary_for_tests():
    """Test-only helper: restore the cached summary to its default values.
    Not for production use."""
    with _ASSEMBLY_HEALTH_LOCK:
        _ASSEMBLY_HEALTH_SUMMARY.clear()
        _ASSEMBLY_HEALTH_SUMMARY.update(_ASSEMBLY_HEALTH_SUMMARY_DEFAULT)


def parse_int_env(name, default, min_value=None):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer")
    if min_value is not None and value < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    return value


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


def _background_drive_upload(job_key: str, output_file):
    try:
        drive_file_id = upload_file_to_drive(output_file)
        job = load_job(D, job_key)
        if job:
            job["drive_file_id"] = drive_file_id
            job["drive_upload_status"] = "done" if drive_file_id else "failed"
            job["updated_at"] = now()
            save_job(D, job_key, job)
        log.info("JOB %s Drive upload done: %s", job_key, drive_file_id)
    except Exception as e:
        log.warning("JOB %s Drive upload failed: %s", job_key, e)
        job = load_job(D, job_key)
        if job:
            job["drive_upload_status"] = "failed"
            job["updated_at"] = now()
            save_job(D, job_key, job)


def _safe_drive_filename(filename) -> str:
    name = (filename or DEFAULT_MCP_IMAGE_FILENAME).strip()
    name = os.path.basename(name.replace("\x00", ""))
    return name or DEFAULT_MCP_IMAGE_FILENAME


def _validate_mcp_jpeg(image_bytes: bytes):
    if not image_bytes:
        raise HTTPException(status_code=400, detail="empty image/jpeg payload")
    if not image_bytes.startswith(b"\xff\xd8"):
        raise HTTPException(status_code=415, detail="payload is not a JPEG file")


def _google_drive_service():
    missing = [
        name
        for name in (
            "GOOGLE_CLIENT_ID",
            "GOOGLE_CLIENT_SECRET",
            "GOOGLE_REFRESH_TOKEN",
        )
        if not os.getenv(name)
    ]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"missing Google Drive OAuth env vars: {', '.join(missing)}",
        )

    credentials = Credentials(
        token=None,
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=[GOOGLE_DRIVE_SCOPE],
    )
    credentials.refresh(GoogleAuthRequest())
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def _upload_image_to_drive_and_share(image_bytes: bytes, filename=None) -> dict:
    _validate_mcp_jpeg(image_bytes)
    safe_name = _safe_drive_filename(filename)
    metadata = {
        "name": safe_name,
        "mimeType": "image/jpeg",
    }
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if folder_id:
        metadata["parents"] = [folder_id]

    media = MediaIoBaseUpload(
        BytesIO(image_bytes),
        mimetype="image/jpeg",
        resumable=False,
    )

    try:
        service = _google_drive_service()
        created = service.files().create(
            body=metadata,
            media_body=media,
            fields="id,name,mimeType,webViewLink",
        ).execute()
        service.permissions().create(
            fileId=created["id"],
            body={"type": "anyone", "role": "reader"},
            fields="id",
        ).execute()
        shared = service.files().get(
            fileId=created["id"],
            fields="id,name,mimeType,webViewLink",
        ).execute()
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("MCP Drive raw image upload failed")
        raise HTTPException(status_code=502, detail=f"drive upload failed: {type(exc).__name__}")

    return {
        "file_id": shared["id"],
        "name": shared.get("name", safe_name),
        "mime_type": shared.get("mimeType", "image/jpeg"),
        "webViewLink": shared.get("webViewLink", ""),
    }


def _mcp_tool_definition() -> dict:
    return {
        "name": MCP_TOOL_NAME,
        "description": (
            "Upload a JPEG image to Google Drive as a raw image file, make it readable "
            "by anyone with the link, and return the public webViewLink. This is a write "
            "action and should be confirmed by the MCP client before execution. It does "
            "not create Google Docs, Slides, or Sheets and does not publish to social media."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "image_base64": {
                    "type": "string",
                    "description": (
                        "Base64-encoded raw image/jpeg bytes. For direct raw upload, "
                        "POST image/jpeg bytes to /mcp?filename=<name>."
                    ),
                },
                "filename": {
                    "type": "string",
                    "description": "Drive file name. Defaults to hafiz_ai_concept_instagram.jpg.",
                    "default": DEFAULT_MCP_IMAGE_FILENAME,
                },
            },
            "required": ["image_base64"],
            "additionalProperties": False,
        },
        "annotations": {
            "title": "Upload JPEG to Drive and share",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    }


def _mcp_response(request_id, result: dict):
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _mcp_error(request_id, code: int, message: str, data=None):
    error = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _decode_mcp_image_base64(value: str) -> bytes:
    if not value:
        raise ValueError("image_base64 is required")
    if "," in value and value.strip().lower().startswith("data:"):
        value = value.split(",", 1)[1]
    try:
        return base64.b64decode(value, validate=True)
    except binascii.Error as exc:
        raise ValueError("image_base64 must be valid base64") from exc


def _handle_mcp_jsonrpc_message(message: dict):
    request_id = message.get("id")
    method = message.get("method")

    if method == "initialize":
        client_protocol = (message.get("params") or {}).get("protocolVersion")
        return _mcp_response(
            request_id,
            {
                "protocolVersion": client_protocol or MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": MCP_SERVER_NAME,
                    "version": MCP_SERVER_VERSION,
                },
            },
        )

    if method == "notifications/initialized":
        return None

    if method == "ping":
        return _mcp_response(request_id, {})

    if method == "tools/list":
        return _mcp_response(request_id, {"tools": [_mcp_tool_definition()]})

    if method == "tools/call":
        params = message.get("params") or {}
        if params.get("name") != MCP_TOOL_NAME:
            return _mcp_error(request_id, -32602, f"unsupported tool: {params.get('name')}")

        arguments = params.get("arguments") or {}
        try:
            image_bytes = _decode_mcp_image_base64(arguments.get("image_base64", ""))
            upload = _upload_image_to_drive_and_share(
                image_bytes=image_bytes,
                filename=arguments.get("filename"),
            )
        except HTTPException as exc:
            return _mcp_error(request_id, exc.status_code, str(exc.detail))
        except ValueError as exc:
            return _mcp_error(request_id, -32602, str(exc))

        text = json.dumps(upload, ensure_ascii=False)
        return _mcp_response(
            request_id,
            {
                "content": [{"type": "text", "text": text}],
                "structuredContent": upload,
                "isError": False,
            },
        )

    return _mcp_error(request_id, -32601, f"method not found: {method}")


@app.get("/mcp")
def mcp_info():
    return {
        "ok": True,
        "server": MCP_SERVER_NAME,
        "version": MCP_SERVER_VERSION,
        "transport": "http",
        "endpoint": "/mcp",
        "tools": [MCP_TOOL_NAME],
    }


@app.post("/mcp")
async def mcp_endpoint(request: Request):
    content_type = request.headers.get("content-type", "").lower()
    if content_type.startswith("image/jpeg"):
        image_bytes = await request.body()
        upload = _upload_image_to_drive_and_share(
            image_bytes=image_bytes,
            filename=request.query_params.get("filename") or request.headers.get("X-HAFIS-Filename"),
        )
        return {
            "ok": True,
            "tool": MCP_TOOL_NAME,
            "result": upload,
        }

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="expected MCP JSON-RPC payload or raw image/jpeg body",
        )

    if isinstance(payload, list):
        responses = [
            response
            for item in payload
            if isinstance(item, dict)
            for response in [_handle_mcp_jsonrpc_message(item)]
            if response is not None
        ]
        if not responses:
            return JSONResponse(status_code=202, content={})
        return responses

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="MCP JSON-RPC payload must be an object or batch")

    response = _handle_mcp_jsonrpc_message(payload)
    if response is None:
        return JSONResponse(status_code=202, content={})
    return response


def process_job(key: str):
    job = load_job(P, key)
    if not job:
        return

    if job.get("kind") == UPLOAD_KIND:
        process_upload_job(
            key,
            P=P, D=D, F=F,
            save_job=save_job, delete_job=delete_job, load_job=load_job,
            now=now, log=log,
        )
        return

    try:
        audio_file = AUD / f"{key}.mp3"
        output_file = OUT / f"{key}.mp4"

        job["status"] = "processing"
        job["job_state_dir"] = "processing"
        job["drive_file_id"] = job.get("drive_file_id", "")
        job["drive_upload_status"] = job.get("drive_upload_status", "pending")
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
        job["drive_file_id"] = ""
        job["drive_upload_status"] = "pending"
        job["error_message"] = ""
        job["updated_at"] = now()
        delete_job(P, key)
        save_job(D, key, job)
        log.info("JOB %s completed", key)

        threading.Thread(
            target=_background_drive_upload,
            args=(key, output_file),
            daemon=True,
        ).start()

    except Exception as e:
        log.exception("JOB %s processing failed", key)
        job = load_job(P, key) or job
        job["status"] = "failed"
        job["job_state_dir"] = "failed"
        job["video_url"] = ""
        job["output_file"] = ""
        job["artifact_ready"] = False
        job["artifact_endpoint"] = ""
        job["drive_file_id"] = job.get("drive_file_id", "")
        job["drive_upload_status"] = job.get("drive_upload_status", "pending")
        job["error_message"] = str(e)
        job["updated_at"] = now()
        delete_job(P, key)
        save_job(F, key, job)


async def reap_stale_jobs():
    while True:
        try:
            timeout_seconds = parse_int_env("STALE_JOB_TIMEOUT_SECONDS", default=3600, min_value=60)
            cutoff = time.time() - timeout_seconds
            F.mkdir(parents=True, exist_ok=True)

            for path in P.glob("*.json"):
                try:
                    if path.stat().st_mtime >= cutoff:
                        continue

                    key = path.stem
                    job = load_job(P, key) or {}
                    job["status"] = "failed"
                    job["job_state_dir"] = "failed"
                    job["error_class"] = "TIMEOUT"
                    job["error_message"] = "Stale job reaped"
                    job["updated_at"] = now()
                    save_job(P, key, job)
                    shutil.move(str(path), str(F / path.name))
                    log.info("JOB %s reaped as stale", key)
                except Exception as e:
                    log.exception("stale job reaper error on %s: %s", path, e)
        except Exception:
            log.exception("stale job reaper loop error")

        await asyncio.sleep(300)


@app.on_event("startup")
async def start_stale_job_reaper():
    asyncio.create_task(reap_stale_jobs())


@app.on_event("startup")
async def run_assembly_startup_scan():
    """
    Kick off Drive-backed assembly reconciliation after the app is ready.
    The scan can perform network I/O and full persisted-state reads, so startup
    must schedule it in the background instead of awaiting it on the readiness
    path.
    """
    _schedule_assembly_startup_scan()


def _schedule_assembly_startup_scan():
    """Schedule the background scan task without awaiting it.
    Indirection makes the non-await behavior easy to assert in tests."""
    return asyncio.create_task(_run_assembly_startup_scan_background())


def worker_loop():
    while True:
        try:
            reap_stale_uploads(
                P=P, F=F,
                save_job=save_job, delete_job=delete_job, load_job=load_job,
                now=now, log=log,
            )
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


def _set_assembly_health_summary(**updates):
    with _ASSEMBLY_HEALTH_LOCK:
        _ASSEMBLY_HEALTH_SUMMARY.update(updates)


def _assembly_local_processing_count() -> int:
    try:
        if ASSEMBLE_PROCESSING.exists():
            return len(list(ASSEMBLE_PROCESSING.glob("*.json")))
    except Exception:
        log.exception("assembly local processing health check failed")
    return 0


def _assembly_health_summary():
    """
    Return the cached assembly persistence summary only.
    Do not call Drive/list_all_states/list_active_persisted from /health.
    """
    with _ASSEMBLY_HEALTH_LOCK:
        summary = dict(_ASSEMBLY_HEALTH_SUMMARY)

    summary["local_processing_count"] = _assembly_local_processing_count()
    if summary.get("persisted_non_terminal_count", 0) > 0:
        summary["reconciled"] = False
    if summary.get("known_unreconciled_count", 0) > 0:
        summary["reconciled"] = False
    return summary


def _assembly_startup_scan_timeout_seconds() -> int:
    return parse_int_env("ASSEMBLY_STARTUP_SCAN_TIMEOUT_SECONDS", default=300, min_value=1)


async def _run_assembly_startup_scan_background():
    _set_assembly_health_summary(
        scan_state="running",
        last_scan_started_at=now(),
        last_scan_completed_at=None,
        last_scan_error="",
    )
    loop = asyncio.get_running_loop()
    scan_future = loop.run_in_executor(None, startup_scan_recover_interrupted)
    try:
        summary = await asyncio.wait_for(
            scan_future,
            timeout=_assembly_startup_scan_timeout_seconds(),
        )
    except asyncio.TimeoutError:
        # The wait_for timeout cancels the awaitable but the underlying
        # executor thread may still continue running its Drive I/O. We mark
        # the cached health state as timed_out and keep readiness unblocked;
        # we do not claim the underlying scan was cancelled.
        _set_assembly_health_summary(
            scan_state="timed_out",
            reconciled=False,
            last_scan_completed_at=now(),
            last_scan_error="TimeoutError",
        )
        log.warning("assembly startup scan timed out; underlying executor work may continue")
        return
    except Exception as exc:
        _set_assembly_health_summary(
            scan_state="failed",
            reconciled=False,
            last_scan_completed_at=now(),
            last_scan_error=type(exc).__name__,
        )
        log.exception("assembly startup scan failed")
        return

    # Successful scan. startup_scan_recover_interrupted() has already moved
    # any non-terminal persisted jobs into the FAILED_RESTART_INTERRUPTED
    # terminal state, so they are no longer unreconciled. We record the
    # interrupted count separately and clear unreconciled / persisted-non-
    # terminal counters so /health does not stay ok=false forever.
    interrupted = int(summary.get("interrupted", 0) or 0)
    _set_assembly_health_summary(
        scan_state="complete",
        persisted_non_terminal_count=0,
        known_unreconciled_count=0,
        known_interrupted_count=interrupted,
        reconciled=True,
        last_scan_completed_at=now(),
        last_scan_error="",
        last_scan_summary=summary,
    )
    log.info("assembly startup scan: %s", summary)


@app.get("/health")
def health():
    render_active_jobs = len(list(Q.glob("*.json")) + list(P.glob("*.json")))
    assembly = _assembly_health_summary()
    assembly_unreconciled = not bool(assembly.get("reconciled", True))
    return {
        "ok": (not assembly_unreconciled),
        "service": "yt-video-render-runner",
        "ffmpeg_ready": shutil.which("ffmpeg") is not None,
        "time": now(),
        "instance_id": _INSTANCE_ID,
        "uptime_seconds": int(time.time() - _BOOT_TIME),
        "active_jobs": render_active_jobs,
        "assembly": assembly,
    }


@app.post("/render-jobs")
def submit_job(req: RenderRequest):
    max_concurrent_renders = int(os.environ.get("MAX_CONCURRENT_RENDERS", "3"))
    active_render_jobs = len(list(Q.glob("*.json")) + list(P.glob("*.json")))
    if active_render_jobs >= max_concurrent_renders:
        return JSONResponse(
            status_code=429,
            content={
                "error_class": "CONCURRENCY",
                "error_message": "Too many active render jobs",
            },
        )

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
        "drive_file_id": "",
        "drive_upload_status": "pending",
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
        "drive_file_id": job.get("drive_file_id", ""),
        "drive_upload_status": job.get("drive_upload_status", "pending"),
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


register_upload_routes(app)
register_assembly_routes(app)
