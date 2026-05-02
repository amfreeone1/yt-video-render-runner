"""
Cinematic assembly jobs module.

Adds POST /assemble-job, GET /assemble-jobs/{job_key}, and
GET /assemble-jobs/{job_key}/video without changing existing render/upload routes.
Uses Pexels for cinematic video clips, Pixabay for ambient music, and FFmpeg
for landscape video assembly.
"""

import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field


BASE = Path(__file__).resolve().parent
JOBS = BASE / "jobs"
ASSEMBLE_JOBS = JOBS / "assemble"
ASSEMBLE_DONE = ASSEMBLE_JOBS / "done"
ASSEMBLE_FAILED = ASSEMBLE_JOBS / "failed"
ASSEMBLE_PROCESSING = ASSEMBLE_JOBS / "processing"
ASSEMBLE_STDERR = ASSEMBLE_JOBS / "stderr"

PEXELS_VIDEO_SEARCH_URL = "https://api.pexels.com/videos/search"
PIXABAY_MUSIC_SEARCH_URL = "https://pixabay.com/api/music/"

DEFAULT_SECTION_QUERIES = {
    "HOOK": "kitchen electricity dark cinematic",
    "PROBLEM": "refrigerator running night timelapse",
    "EXPLANATION": "compressor machinery close up cinematic",
    "SOLUTION": "clean kitchen modern energy",
    "CTA": "modern refrigerator lifestyle",
}


class ScriptSection(BaseModel):
    section: Optional[str] = None
    label: Optional[str] = None
    title: Optional[str] = None
    text: str = ""
    query: Optional[str] = None


class AssembleJobRequest(BaseModel):
    content_id: str
    audio_url: str
    script_sections: List[ScriptSection] = Field(default_factory=list)


class FFmpegCommandError(RuntimeError):
    def __init__(self, message: str, *, returncode: int, stderr_path: Optional[Path] = None):
        super().__init__(message)
        self.returncode = returncode
        self.stderr_path = stderr_path


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or uuid.uuid4().hex


def _job_path(folder: Path, job_key: str) -> Path:
    return folder / f"{job_key}.json"


def _stderr_path(job_key: str) -> Path:
    return ASSEMBLE_STDERR / f"{job_key}.stderr.log"


def _append_stderr(job_key: Optional[str], stderr: str) -> Optional[Path]:
    if not job_key or not stderr:
        return None
    ASSEMBLE_STDERR.mkdir(parents=True, exist_ok=True)
    path = _stderr_path(job_key)
    with path.open("a", encoding="utf-8", errors="replace") as handle:
        handle.write(stderr)
        if not stderr.endswith("\n"):
            handle.write("\n")
    return path


def _stderr_tail(job_key: str, lines: int = 80) -> Optional[List[str]]:
    path = _stderr_path(job_key)
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]


def _log_event(event: str, job_key: str, **fields: Any) -> None:
    payload = {"event": event, "job_key": job_key, "ts": _now(), **fields}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def _save_job(folder: Path, job_key: str, data: Dict[str, Any]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    tmp = _job_path(folder, job_key).with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, _job_path(folder, job_key))


def _load_job(folder: Path, job_key: str) -> Optional[Dict[str, Any]]:
    path = _job_path(folder, job_key)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _find_job(job_key: str) -> Optional[Dict[str, Any]]:
    for folder in (ASSEMBLE_PROCESSING, ASSEMBLE_DONE, ASSEMBLE_FAILED):
        job = _load_job(folder, job_key)
        if job:
            return job
    return None


def _check_auth(x_runner_auth: Optional[str]) -> None:
    expected = os.environ.get("RUNNER_AUTH_TOKEN")
    if not expected or x_runner_auth != expected:
        raise HTTPException(
            status_code=401,
            detail={"error_class": "AUTH", "error_message": "Unauthorized"},
        )


def _build_audio_download_url(url: str) -> str:
    match = re.search(r"/d/([A-Za-z0-9_-]+)", url or "")
    if match:
        file_id = match.group(1)
        return f"https://drive.google.com/uc?export=download&confirm=t&id={urllib.parse.quote(file_id)}"
    return url


def _download_url(url: str, dest: Path, *, headers: Optional[Dict[str, str]] = None, timeout: int = 300) -> None:
    with requests.get(url, stream=True, allow_redirects=True, timeout=timeout, headers=headers or {}) as response:
        response.raise_for_status()
        with dest.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    handle.write(chunk)


def _run(cmd: List[str], *, job_key: Optional[str] = None, event: Optional[str] = None) -> subprocess.CompletedProcess:
    if event and job_key:
        _log_event("FFMPEG_START", job_key, command=event)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    stderr_path = _append_stderr(job_key, result.stderr or "")
    if result.returncode != 0:
        if job_key:
            _log_event(
                "FFMPEG_FAILED",
                job_key,
                command=event or cmd[0],
                returncode=result.returncode,
                stderr_path=str(stderr_path) if stderr_path else None,
            )
        message = (result.stderr or result.stdout or "ffmpeg command failed")[:1000]
        raise FFmpegCommandError(message, returncode=result.returncode, stderr_path=stderr_path)
    return result


def _probe_duration(path: Path, *, job_key: Optional[str] = None) -> float:
    result = _run([
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(path),
    ], job_key=job_key, event="probe_duration")
    return max(float((result.stdout or "0").strip() or "0"), 0.1)


def _section_name(section: ScriptSection) -> str:
    return (section.section or section.label or section.title or "SECTION").strip().upper()


def _section_text(section: ScriptSection) -> str:
    name = _section_name(section)
    text = section.text.strip()
    return f"{name}: {text}" if text else name


def _section_query(section: ScriptSection) -> str:
    name = _section_name(section)
    if section.query:
        return section.query
    if name in DEFAULT_SECTION_QUERIES:
        return DEFAULT_SECTION_QUERIES[name]
    if section.text:
        return f"{section.text[:80]} cinematic landscape"
    return "cinematic landscape"


def _pexels_video_url(query: str) -> str:
    api_key = os.environ.get("PEXELS_API_KEY")
    if not api_key:
        raise RuntimeError("PEXELS_API_KEY env var not set")

    response = requests.get(
        PEXELS_VIDEO_SEARCH_URL,
        headers={"Authorization": api_key},
        params={"query": query, "orientation": "landscape", "per_page": 5},
        timeout=30,
    )
    response.raise_for_status()
    videos = response.json().get("videos", [])
    if not videos:
        raise RuntimeError(f"no Pexels videos found for query={query}")

    files = videos[0].get("video_files", [])
    mp4_files = [item for item in files if item.get("file_type") == "video/mp4" and item.get("link")]
    if not mp4_files:
        raise RuntimeError(f"no Pexels mp4 video found for query={query}")

    mp4_files.sort(key=lambda item: abs((item.get("width") or 1920) - 1920))
    return mp4_files[0]["link"]


def _pixabay_music_url() -> Optional[str]:
    api_key = os.environ.get("PIXABAY_API_KEY")
    if not api_key:
        raise RuntimeError("PIXABAY_API_KEY env var not set")

    response = requests.get(
        PIXABAY_MUSIC_SEARCH_URL,
        params={"key": api_key, "q": "cinematic ambient", "per_page": 3, "safesearch": "true"},
        timeout=30,
    )
    response.raise_for_status()
    hits = response.json().get("hits", [])
    if not hits:
        return None

    first = hits[0]
    return first.get("audio") or first.get("previewURL")


def _prepare_video_segment(input_path: Path, output_path: Path, duration: float, text: str, *, job_key: str) -> None:
    safe_text = text.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")[:180]
    drawtext = (
        "drawtext="
        "fontcolor=white:fontsize=38:box=1:boxcolor=black@0.35:boxborderw=18:"
        f"text='{safe_text}':x=80:y=h-150"
    )
    vf = f"scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,{drawtext}"
    _run([
        "ffmpeg",
        "-y",
        "-stream_loop", "-1",
        "-i", str(input_path),
        "-t", f"{duration:.3f}",
        "-vf", vf,
        "-an",
        "-r", "30",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        str(output_path),
    ], job_key=job_key, event="prepare_video_segment")


def _concat_segments(segment_paths: List[Path], output_path: Path, work_dir: Path, *, job_key: str) -> None:
    concat_file = work_dir / "segments.txt"
    concat_file.write_text("".join(f"file '{path.as_posix()}'\n" for path in segment_paths), encoding="utf-8")
    _run([
        "ffmpeg",
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_file),
        "-c", "copy",
        str(output_path),
    ], job_key=job_key, event="concat_segments")


def _mix_audio(video_path: Path, voice_path: Path, music_path: Optional[Path], output_path: Path, duration: float, *, job_key: str) -> None:
    if music_path and music_path.exists():
        _run([
            "ffmpeg",
            "-y",
            "-i", str(video_path),
            "-i", str(voice_path),
            "-stream_loop", "-1",
            "-i", str(music_path),
            "-filter_complex", "[1:a]volume=1.0[a0];[2:a]volume=0.15[a1];[a0][a1]amix=inputs=2:duration=first:dropout_transition=2[aout]",
            "-map", "0:v:0",
            "-map", "[aout]",
            "-t", f"{duration:.3f}",
            "-c:v", "copy",
            "-c:a", "aac",
            "-shortest",
            str(output_path),
        ], job_key=job_key, event="mix_audio_with_music")
    else:
        _run([
            "ffmpeg",
            "-y",
            "-i", str(video_path),
            "-i", str(voice_path),
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-t", f"{duration:.3f}",
            "-c:v", "copy",
            "-c:a", "aac",
            "-shortest",
            str(output_path),
        ], job_key=job_key, event="mix_audio_voice_only")


def _process_assemble_job(job_key: str, body: AssembleJobRequest) -> None:
    job = _load_job(ASSEMBLE_PROCESSING, job_key)
    if not job:
        return

    _log_event("ASSEMBLY_PICKUP", job_key, content_id=body.content_id)
    output_path = Path(f"/tmp/output_{_safe_key(body.content_id)}.mp4")
    try:
        with tempfile.TemporaryDirectory(prefix=f"assemble_{job_key}_") as tmp:
            work_dir = Path(tmp)
            voice_path = work_dir / "voice_audio"
            _log_event("AUDIO_FETCH_START", job_key, source="voice")
            _download_url(_build_audio_download_url(body.audio_url), voice_path)
            _log_event("AUDIO_FETCH_DONE", job_key, source="voice", bytes=voice_path.stat().st_size)
            voice_duration = _probe_duration(voice_path, job_key=job_key)

            sections = body.script_sections or [ScriptSection(section="VIDEO", text="")]
            section_duration = max(voice_duration / len(sections), 1.0)

            segment_paths: List[Path] = []
            for index, section in enumerate(sections):
                clip_url = _pexels_video_url(_section_query(section))
                clip_path = work_dir / f"clip_{index}.mp4"
                segment_path = work_dir / f"segment_{index}.mp4"
                _download_url(clip_url, clip_path)
                _prepare_video_segment(clip_path, segment_path, section_duration, _section_text(section), job_key=job_key)
                segment_paths.append(segment_path)

            concat_video_path = work_dir / "concat_video.mp4"
            _concat_segments(segment_paths, concat_video_path, work_dir, job_key=job_key)

            music_path: Optional[Path] = None
            music_url = _pixabay_music_url()
            if music_url:
                music_path = work_dir / "music_audio"
                _log_event("AUDIO_FETCH_START", job_key, source="music")
                _download_url(music_url, music_path)
                _log_event("AUDIO_FETCH_DONE", job_key, source="music", bytes=music_path.stat().st_size)

            _mix_audio(concat_video_path, voice_path, music_path, output_path, voice_duration, job_key=job_key)

        job["status"] = "done"
        job["output_path"] = str(output_path)
        job["output_video_url"] = f"/assemble-jobs/{job_key}/video"
        job["updated_at"] = _now()
        _save_job(ASSEMBLE_DONE, job_key, job)
        _job_path(ASSEMBLE_PROCESSING, job_key).unlink(missing_ok=True)
    except Exception as exc:
        job["status"] = "failed"
        job["error_class"] = "ASSEMBLY"
        job["error_message"] = str(exc)[:500]
        if isinstance(exc, FFmpegCommandError):
            job["ffmpeg_returncode"] = exc.returncode
            if exc.stderr_path:
                job["ffmpeg_stderr_path"] = str(exc.stderr_path)
        job["ffmpeg_stderr_tail"] = _stderr_tail(job_key) or []
        job["updated_at"] = _now()
        _save_job(ASSEMBLE_FAILED, job_key, job)
        _log_event(
            "STATE_FAILED_WRITTEN",
            job_key,
            error_class=job.get("error_class"),
            ffmpeg_returncode=job.get("ffmpeg_returncode"),
        )
        _job_path(ASSEMBLE_PROCESSING, job_key).unlink(missing_ok=True)


def register_assembly_routes(app: FastAPI) -> None:
    for folder in (ASSEMBLE_PROCESSING, ASSEMBLE_DONE, ASSEMBLE_FAILED, ASSEMBLE_STDERR):
        folder.mkdir(parents=True, exist_ok=True)

    @app.post("/assemble-job", status_code=202)
    def create_assemble_job(
        body: AssembleJobRequest,
        x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
    ):
        _check_auth(x_runner_auth)
        if not body.content_id:
            raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "content_id is required"})
        if not body.audio_url:
            raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "audio_url is required"})
        if not body.script_sections:
            raise HTTPException(status_code=400, detail={"error_class": "VALIDATION", "error_message": "script_sections is required"})

        job_key = f"assemble_{_safe_key(body.content_id)}_{uuid.uuid4().hex[:8]}"
        job = {
            "job_key": job_key,
            "content_id": body.content_id,
            "status": "processing",
            "output_video_url": None,
            "output_path": None,
            "error_class": None,
            "error_message": None,
            "ffmpeg_returncode": None,
            "ffmpeg_stderr_path": None,
            "ffmpeg_stderr_tail": None,
            "received_at": _now(),
            "updated_at": _now(),
        }
        _save_job(ASSEMBLE_PROCESSING, job_key, job)
        threading.Thread(target=_process_assemble_job, args=(job_key, body), daemon=True).start()
        return {"job_key": job_key, "status": "processing"}

    @app.get("/assemble-jobs/{job_key}")
    def get_assemble_job(
        job_key: str,
        x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
    ):
        _check_auth(x_runner_auth)
        job = _find_job(job_key)
        if not job:
            raise HTTPException(status_code=404, detail={"error_class": "NOT_FOUND", "error_message": "assemble job not found"})
        stderr_tail = None
        if job.get("status") == "failed":
            if "ffmpeg_stderr_tail" in job:
                stderr_tail = job.get("ffmpeg_stderr_tail")
            else:
                stderr_tail = _stderr_tail(job_key) or []
        return {
            "job_key": job["job_key"],
            "status": job["status"],
            "output_video_url": job.get("output_video_url"),
            "error_class": job.get("error_class"),
            "error_message": job.get("error_message"),
            "ffmpeg_returncode": job.get("ffmpeg_returncode"),
            "ffmpeg_stderr_tail": stderr_tail,
            "updated_at": job.get("updated_at"),
        }

    @app.get("/assemble-jobs/{job_key}/video")
    def get_assemble_video(
        job_key: str,
        x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
    ):
        _check_auth(x_runner_auth)
        job = _load_job(ASSEMBLE_DONE, job_key)
        if not job or not job.get("output_path"):
            raise HTTPException(status_code=404, detail={"error_class": "NOT_FOUND", "error_message": "assemble video not found"})
        path = Path(job["output_path"])
        if not path.exists():
            raise HTTPException(status_code=404, detail={"error_class": "NOT_FOUND", "error_message": "assemble video file missing"})
        return FileResponse(path, media_type="video/mp4", filename=path.name)
