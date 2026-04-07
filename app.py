from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from pathlib import Path
from datetime import datetime, timezone
import json, uuid, shutil, logging, subprocess, threading, urllib.request

app = FastAPI()
log = logging.getLogger("uvicorn.error")

BASE = Path(__file__).resolve().parent
JOBS = BASE / "jobs"
Q = JOBS / "queued"
P = JOBS / "processing"
D = JOBS / "done"
F = JOBS / "failed"

for d in [Q, P, D, F]:
    d.mkdir(parents=True, exist_ok=True)

# ---------------- REQUEST MODEL ----------------
class RenderRequest(BaseModel):
    render_job_key: str
    audio_url: str
    content_id: str
    source_row_number: int

# ---------------- HELPERS ----------------
def now():
    return datetime.now(timezone.utc).isoformat()

def job_path(folder, key):
    return folder / f"{key}.json"

def save_job(folder, key, data):
    with open(job_path(folder, key), "w") as f:
        json.dump(data, f, indent=2)

def load_job(folder, key):
    path = job_path(folder, key)
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)

def move_job(src, dst, key):
    shutil.move(job_path(src, key), job_path(dst, key))

# ---------------- DOWNLOAD ----------------
def download_file(url, dest):
    urllib.request.urlretrieve(url, dest)

# ---------------- RENDER WORKER ----------------
def process_job(job):
    key = job["render_job_key"]

    try:
        audio_file = P / f"{key}.mp3"
        output_file = P / f"{key}.mp4"

        # download audio
        download_file(job["audio_url"], audio_file)

        # FFmpeg render (simple black video + audio)
        cmd = [
            "ffmpeg",
            "-y",
            "-f", "lavfi",
            "-i", "color=c=black:s=1280x720:d=10",
            "-i", str(audio_file),
            "-shortest",
            "-c:v", "libx264",
            "-c:a", "aac",
            str(output_file)
        ]

        subprocess.run(cmd, check=True)

        job["status"] = "completed"
        job["video_url"] = f"/render-jobs/{key}/video"
        job["output_file"] = f"{key}.mp4"
        job["artifact_ready"] = True
        job["updated_at"] = now()

        save_job(D, key, job)

    except Exception as e:
        job["status"] = "failed"
        job["error_message"] = str(e)
        job["updated_at"] = now()
        save_job(F, key, job)

# ---------------- BACKGROUND LOOP ----------------
def worker_loop():
    while True:
        for file in Q.glob("*.json"):
            key = file.stem
            job = load_job(Q, key)

            if not job:
                continue

            move_job(Q, P, key)
            save_job(P, key, job)

            threading.Thread(target=process_job, args=(job,)).start()

        import time
        time.sleep(5)

threading.Thread(target=worker_loop, daemon=True).start()

# ---------------- API ----------------

@app.post("/render-jobs")
def submit_job(req: RenderRequest):
    job = {
        "found": True,
        "render_job_key": req.render_job_key,
        "job_id": f"job_{uuid.uuid4().hex[:8]}",
        "status": "queued",
        "job_state_dir": "queued",
        "source_row_number": req.source_row_number,
        "content_id": req.content_id,
        "video_url": "",
        "output_file": "",
        "artifact_ready": False,
        "error_message": "",
        "received_at": now(),
        "updated_at": now()
    }

    save_job(Q, req.render_job_key, job)
    return job

@app.get("/render-jobs/{key}")
def get_job(key: str):
    for folder, state in [(Q,"queued"), (P,"processing"), (D,"completed"), (F,"failed")]:
        job = load_job(folder, key)
        if job:
            job["job_state_dir"] = state
            return job

    return JSONResponse(status_code=404, content={"detail": "render job not found"})

@app.get("/render-jobs/{key}/video")
def get_video(key: str):
    video = D / f"{key}.mp4"
    if not video.exists():
        raise HTTPException(status_code=404, detail="Video not ready")
    return FileResponse(video, media_type="video/mp4")
