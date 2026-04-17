from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, FileResponse
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

for d in [Q, P, D, F, OUT, AUD]:
    d.mkdir(parents=True, exist_ok=True)


def _recover_orphaned_processing_jobs():
    """
    Boot-time recovery: any job left in processing/ from a previous
    instance is orphaned (the thread that owned it is dead). Move it
    back to queued/ so the worker_loop will re-pick it up.

    Idempotent: safe to run on every boot. Never touches done/ or failed/.
    """
    recovered = 0
    for file in list(P.glob("*.json")):
        key = file.stem
        try:
            raw = file.read_text(encoding="utf-8")
            job = json.loads(raw)
        except Exception:
            # Corrupt job file — leave it in processing/ for manual inspection
            # rather than silently discarding it. Do NOT move to failed/:
            # that would mutate a row's terminal state without proof.
            log.exception("RECOVERY skipped corrupt job file %s", file.name)
            continue

        job["status"] = "queued"
        job["job_state_dir"] = "queued"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        # Preserve: render_job_key, source_row_number, content_id, audio_url,
        # received_at, job_id. These are the canonical identity fields.

        # Write to queued/ FIRST, then delete from processing/. If we crash
        # between these two steps, we get a duplicate in queued/ (safe:
        # worker_loop will re-pick it) rather than losing the job.
        (Q / file.name).write_text(
            json.dumps(job, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        file.unlink()
        recovered += 1
        log.info("RECOVERY moved orphan %s processing -> queued", key)

    if recovered:
        log.info("RECOVERY complete recovered=%d", recovered)


_recover_orphaned_processing_jobs()

log.info(f"Runner started instance_id={_INSTANCE_ID} active_jobs={len(list(Q.glob('*.json')) + list(P.glob('*.json')))}")


class RenderRequest(BaseModel):
    render_job_key: str
    audio_url: str
    content_id: str
    source_row_number: int


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
        raise RuntimeError(
            f"downloaded_audio_too_small: {size} bytes"
        )

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
            raise RuntimeError(
                f"ffprobe_invalid_audio: {probe.stderr.strip()[:200]}"
            )


def download_file(url: str, dest: Path):
    req = urllib.request.Request(
        build_audio_download_url(url),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=180) as r, dest.open("wb") as f:
        shutil.copyfileobj(r, f)


def process_job(key: str):
    job = load_job(P, key)
    if not job:
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
            raise RuntimeError(result.stderr.strip() or "ffmpeg failed")
        if not output_file.exists() or output_file.stat().st_size == 0:
            raise RuntimeError("output artifact missing")
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
        "active_jobs": len(list(Q.glob("*.json")) + list(P.glob("*.json"))),
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
