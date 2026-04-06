from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
from datetime import datetime, timezone
import json
import uuid
import shutil

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent
JOBS_DIR = BASE_DIR / "jobs"
QUEUED_DIR = JOBS_DIR / "queued"
PROCESSING_DIR = JOBS_DIR / "processing"
DONE_DIR = JOBS_DIR / "done"
FAILED_DIR = JOBS_DIR / "failed"

for d in [QUEUED_DIR, PROCESSING_DIR, DONE_DIR, FAILED_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RenderJob(BaseModel):
    job_type: str
    render_job_key: str
    render_output_name: str
    source_row_number: int
    content_id: str
    topic: str
    audio_url: str
    drive_file_id: str
    drive_file_name: str
    yt_title: str
    yt_description: str
    script_draft: str
    thumbnail_brief: str
    canvas_width: int
    canvas_height: int
    fps: int
    aspect_ratio: str
    video_format: str
    ffmpeg_profile: str
    caption_mode: str
    subtitle_source: str
    visual_strategy: str
    output_storage_strategy: str
    renderer_strategy: str
    runner_type: str
    title_safe_slug: str
    spec_version: str


@app.get("/health")
def health():
    ffmpeg_ready = shutil.which("ffmpeg") is not None
    return {
        "ok": True,
        "service": "yt-video-render-runner",
        "ffmpeg_ready": ffmpeg_ready,
        "time": now_iso(),
    }


@app.post("/render-jobs")
def render_jobs(job: RenderJob):
    if job.renderer_strategy != "ffmpeg_self_hosted":
        raise HTTPException(status_code=400, detail="renderer_strategy must be ffmpeg_self_hosted")

    if job.runner_type != "self_hosted":
        raise HTTPException(status_code=400, detail="runner_type must be self_hosted")

    job_id = f"job_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    payload = job.model_dump()
    payload["job_id"] = job_id
    payload["status"] = "queued"
    payload["received_at"] = now_iso()

    out_file = QUEUED_DIR / f"{job_id}.json"
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return JSONResponse(
        status_code=202,
        content={
            "accepted": True,
            "job_id": job_id,
            "status": "queued",
            "render_job_key": job.render_job_key,
            "source_row_number": job.source_row_number,
            "received_at": payload["received_at"],
        },
    )


def load_job_file(file_path: Path) -> dict:
    return json.loads(file_path.read_text(encoding="utf-8"))


def find_job_by_render_key(render_job_key: str) -> dict | None:
    search_order = [
        ("queued", QUEUED_DIR),
        ("processing", PROCESSING_DIR),
        ("done", DONE_DIR),
        ("failed", FAILED_DIR),
    ]

    for dir_status, dir_path in search_order:
        for file_path in sorted(dir_path.glob("*.json"), reverse=True):
            payload = load_job_file(file_path)
            if payload.get("render_job_key") == render_job_key:
                payload["job_state_dir"] = dir_status
                if not payload.get("status"):
                    payload["status"] = dir_status
                return payload

    return None


@app.get("/render-jobs/{render_job_key}")
def get_render_job(render_job_key: str):
    payload = find_job_by_render_key(render_job_key)

    if not payload:
        raise HTTPException(status_code=404, detail="render job not found")

    return {
        "found": True,
        "render_job_key": render_job_key,
        "job_id": payload.get("job_id", ""),
        "status": payload.get("status", ""),
        "job_state_dir": payload.get("job_state_dir", ""),
        "source_row_number": payload.get("source_row_number"),
        "content_id": payload.get("content_id", ""),
        "video_url": payload.get("video_url", ""),
        "output_file": payload.get("output_file", ""),
        "error_message": payload.get("error_message", ""),
        "received_at": payload.get("received_at", ""),
        "updated_at": payload.get("updated_at", payload.get("received_at", "")),
    }
