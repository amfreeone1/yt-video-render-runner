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
