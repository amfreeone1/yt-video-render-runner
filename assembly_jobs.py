"""
Cinematic assembly jobs module.

Adds POST /assemble-job, GET /assemble-jobs/{job_key}, and
GET /assemble-jobs/{job_key}/video without changing existing render/upload routes.
Uses Pexels for cinematic video clips, Pixabay for ambient music, and FFmpeg
for landscape video assembly.

Restart-safe: every lifecycle transition is mirrored to Google Drive via
utils.state_store so jobs survive Render instance recycles.
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

from utils import state_store


BASE = Path(__file__).resolve().parent
JOBS = BASE / "jobs"
ASSEMBLE_JOBS = JOBS / "assemble"
ASSEMBLE_DONE = ASSEMBLE_JOBS / "done"
ASSEMBLE_FAILED = ASSEMBLE_JOBS / "failed"
ASSEMBLE_PROCESSING = ASSEMBLE_JOBS / "processing"
ASSEMBLE_STDERR = ASSEMBLE_JOBS / "stderr"
ASSEMBLE_OUTPUTS = ASSEMBLE_JOBS / "outputs"

PEXELS_VIDEO_SEARCH_URL = "https://api.pexels.com/videos/search"
PIXABAY_MUSIC_SEARCH_URL = "https://pixabay.com/api/music/"
ASSEMBLE_SEGMENT_WIDTH = 1280
ASSEMBLE_SEGMENT_HEIGHT = 720
ASSEMBLE_FFMPEG_THREADS = "1"
ASSEMBLE_OVERLAY_TEXT_LIMIT = 60

# Lifecycle states — single source of truth for restart-safe persistence.
STATE_QUEUED = "QUEUED"
STATE_STARTED = "STARTED"
STATE_SEGMENT_PREP_START = "SEGMENT_PREP_START"
STATE_SEGMENT_PREP_DONE = "SEGMENT_PREP_DONE"
STATE_FINAL_MUX_START = "FINAL_MUX_START"
STATE_FINAL_MUX_DONE = "FINAL_MUX_DONE"
STATE_COMPLETE_WRITTEN = "STATE_COMPLETE_WRITTEN"
STATE_FAILED = "FAILED"
STATE_FAILED_RESTART_INTERRUPTED = "FAILED_RESTART_INTERRUPTED"

TERMINAL_STATES = {
    STATE_COMPLETE_WRITTEN,
    STATE_FAILED,
    STATE_FAILED_RESTART_INTERRUPTED,
}

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


class ArtifactMirrorError(RuntimeError):
    pass


class StateMirrorError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or uuid.uuid4().hex


def _job_path(folder: Path, job_key: str) -> Path:
    return folder / f"{job_key}.json"


def _stderr_path(job_key: str) -> Path:
    return ASSEMBLE_STDERR / f"{job_key}.stderr.log"


def _output_path(job_key: str) -> Path:
    return ASSEMBLE_OUTPUTS / f"{_safe_key(job_key)}.mp4"


def _safe_log_path(path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    return _sanitize_external_error(str(path))


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
    # Local miss — fall through to Drive mirror so post-restart GETs work.
    persisted = state_store.read_state(job_key)
    if persisted:
        _log_event(
            "STATE_RESTORED_FROM_DRIVE",
            job_key,
            lifecycle_state=persisted.get("lifecycle_state"),
        )
        return persisted
    return None


def _mirror_state(job_key: str, job: Dict[str, Any]) -> str:
    """Best-effort mirror of job state to Drive. Returns mirror id or empty string."""
    try:
        return state_store.write_state(job_key, job) or ""
    except Exception as exc:
        _log_event(
            "STATE_MIRROR_FAILED",
            job_key,
            error_message=_sanitize_external_error(exc),
        )
        return ""


def _persist_lifecycle(
    job_key: str,
    lifecycle_state: str,
    folder: Path,
    job: Dict[str, Any],
    *,
    require_mirror: bool = False,
    **extra: Any,
) -> str:
    """
    Single chokepoint for lifecycle transitions:
      1. Update lifecycle_state + updated_at + extras.
      2. Write locally (existing fast-cache idiom).
      3. Mirror to Drive (durable across restart).
      4. Emit a single LIFECYCLE_<STATE> log event.

    This is in addition to the existing per-step events
    (SEGMENT_PREP_START, FINAL_MUX_DONE, STATE_COMPLETE_WRITTEN, etc.)
    which the existing test suite asserts on. We do not replace them.
    """
    job["lifecycle_state"] = lifecycle_state
    job["updated_at"] = _now()
    for k, v in extra.items():
        job[k] = v
    _save_job(folder, job_key, job)
    mirror_id = _mirror_state(job_key, job)
    if require_mirror and state_store.is_enabled() and not mirror_id:
        raise StateMirrorError(f"could not persist lifecycle state {lifecycle_state}")
    _log_event(
        f"LIFECYCLE_{lifecycle_state}",
        job_key,
        content_id=job.get("content_id"),
        status=job.get("status"),
    )
    return mirror_id


def list_active_persisted() -> List[Dict[str, Any]]:
    """Persisted (Drive) state records whose lifecycle is non-terminal."""
    if not state_store.is_enabled():
        return []
    return [
        s for s in state_store.list_all_states()
        if s.get("lifecycle_state") not in TERMINAL_STATES
    ]


def startup_scan_recover_interrupted() -> Dict[str, int]:
    """
    Called once on FastAPI startup. Reads every persisted state from Drive.
    Any non-terminal state is marked FAILED_RESTART_INTERRUPTED so it is
    no longer "in flight" from an operator's perspective. Returns counts.
    """
    if not state_store.is_enabled():
        _log_event(
            "STARTUP_SCAN_SKIPPED",
            job_key="*",
            reason="state_store_disabled",
        )
        return {"scanned": 0, "interrupted": 0, "terminal": 0}

    states = state_store.list_all_states()
    interrupted = 0
    terminal = 0
    for state in states:
        job_key = state.get("job_key")
        lifecycle = state.get("lifecycle_state")
        if not job_key:
            continue
        if lifecycle in TERMINAL_STATES:
            terminal += 1
            continue
        previous = lifecycle
        state["lifecycle_state"] = STATE_FAILED_RESTART_INTERRUPTED
        state["status"] = "failed"
        state["error_class"] = "RESTART_INTERRUPTED"
        state["error_message"] = (
            f"job was in state {previous} when Render instance recycled; "
            "no artifact produced"
        )
        state["updated_at"] = _now()
        _save_job(ASSEMBLE_FAILED, job_key, state)
        _mirror_state(job_key, state)
        _log_event(
            f"LIFECYCLE_{STATE_FAILED_RESTART_INTERRUPTED}",
            job_key,
            previous_lifecycle_state=previous,
        )
        interrupted += 1

    summary = {
        "scanned": len(states),
        "interrupted": interrupted,
        "terminal": terminal,
    }
    _log_event("STARTUP_SCAN_COMPLETE", job_key="*", **summary)
    return summary


def _check_auth(x_runner_auth: Optional[str]) -> None:
    expected = os.environ.get("RUNNER" + "_AUTH_TOKEN")
    if not expected or x_runner_auth != expected:
        raise HTTPException(
            status_code=401,
            detail={"error_class": "AUTH", "error_message": "Unauthorized"},
        )


def _sanitize_external_error(value: Any) -> str:
    message = str(value)
    message = re.sub(r"([?&](?:key|token|access_token|signature|X-Goog-Signature)=)[^&\s]+", r"\1<redacted>", message, flags=re.IGNORECASE)
    message = re.sub(r"(Authorization(?:%3A|:)?\s*(?:Bearer\s+)?)[A-Za-z0-9._~+/=-]+", r"\1<redacted>", message, flags=re.IGNORECASE)
    return message


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


def _section_overlay_text(section: ScriptSection) -> str:
    for value in (section.title, section.label, section.section):
        text = (value or "").strip()
        if text:
            return text
    return "SECTION"


def _section_text(section: ScriptSection) -> str:
    return _section_overlay_text(section)


def _sanitize_drawtext_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text.replace(chr(0), "")).strip()
    return text[:ASSEMBLE_OVERLAY_TEXT_LIMIT]


def _write_drawtext_textfile(work_dir: Path, index: int, text: str) -> Path:
    textfile_path = work_dir / f"overlay_text_{index}.txt"
    textfile_path.write_text(_sanitize_drawtext_text(text), encoding="utf-8")
    return textfile_path


def _drawtext_textfile_value(path: Path) -> str:
    return path.as_posix().replace("\\", "\\\\").replace(":", "\\:")


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
    api_key = os.environ.get("PEXELS" + "_API_KEY")
    if not api_key:
        raise RuntimeError(f"{'PEXELS' + '_API_KEY'} env var not set")

    response = requests.get(
        PEXELS_VIDEO_SEARCH_URL,
        headers={"Author" + "ization": api_key},
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

    mp4_files.sort(key=lambda item: abs((item.get("width") or ASSEMBLE_SEGMENT_WIDTH) - ASSEMBLE_SEGMENT_WIDTH))
    return mp4_files[0]["link"]


def _pixabay_music_url(*, job_key: Optional[str] = None) -> Optional[str]:
    api_key = os.environ.get("PIXABAY" + "_API_KEY")
    if not api_key:
        if job_key:
            _log_event("MUSIC_SKIPPED", job_key, reason="pixabay_api_key_missing")
        return None

    if job_key:
        _log_event("MUSIC_FETCH_START", job_key, provider="pixabay")

    try:
        response = requests.get(
            PIXABAY_MUSIC_SEARCH_URL,
            params={"key": api_key, "q": "cinematic ambient", "per_page": 3, "safesearch": "true"},
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        if job_key:
            _log_event(
                "MUSIC_FETCH_FAILED_NONFATAL",
                job_key,
                provider="pixabay",
                error_message=_sanitize_external_error(exc),
            )
            _log_event("MUSIC_SKIPPED", job_key, reason="pixabay_fetch_failed")
        return None

    hits = response.json().get("hits", [])
    if not hits:
        if job_key:
            _log_event("MUSIC_SKIPPED", job_key, reason="pixabay_no_hits")
        return None

    first = hits[0]
    music_url = first.get("audio") or first.get("previewURL")
    if not music_url:
        if job_key:
            _log_event("MUSIC_SKIPPED", job_key, reason="pixabay_no_audio_url")
        return None
    return music_url


def _prepare_video_segment(input_path: Path, output_path: Path, duration: float, textfile_path: Path, *, job_key: str) -> None:
    drawtext = (
        "drawtext="
        "fontcolor=white:fontsize=38:box=1:boxcolor=black@0.35:boxborderw=18:"
        f"textfile={_drawtext_textfile_value(textfile_path)}:x=80:y=h-150"
    )
    vf = (
        f"scale={ASSEMBLE_SEGMENT_WIDTH}:{ASSEMBLE_SEGMENT_HEIGHT}:force_original_aspect_ratio=increase,"
        f"crop={ASSEMBLE_SEGMENT_WIDTH}:{ASSEMBLE_SEGMENT_HEIGHT},{drawtext}"
    )
    _log_event(
        "SEGMENT_PREP_START",
        job_key,
        input_path=str(input_path),
        output_path=str(output_path),
        duration=f"{duration:.3f}",
    )
    _run([
        "ffmpeg",
        "-y",
        "-threads", ASSEMBLE_FFMPEG_THREADS,
        "-filter_threads", ASSEMBLE_FFMPEG_THREADS,
        "-stream_loop", "-1",
        "-i", str(input_path),
        "-t", f"{duration:.3f}",
        "-vf", vf,
        "-an",
        "-r", "30",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-threads", ASSEMBLE_FFMPEG_THREADS,
        str(output_path),
    ], job_key=job_key, event="prepare_video_segment")
    _log_event(
        "SEGMENT_PREP_DONE",
        job_key,
        output_path=str(output_path),
        bytes=output_path.stat().st_size if output_path.exists() else None,
    )


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

    _persist_lifecycle(
        job_key, STATE_STARTED, ASSEMBLE_PROCESSING, job,
        current_step="started",
    )
    _log_event("ASSEMBLY_PICKUP", job_key, content_id=body.content_id)
    output_path = _output_path(job_key)
    ASSEMBLE_OUTPUTS.mkdir(parents=True, exist_ok=True)
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

            _persist_lifecycle(
                job_key, STATE_SEGMENT_PREP_START, ASSEMBLE_PROCESSING, job,
                current_step="segment_prep",
                segment_count=len(sections),
            )

            segment_paths: List[Path] = []
            for index, section in enumerate(sections):
                clip_url = _pexels_video_url(_section_query(section))
                clip_path = work_dir / f"clip_{index}.mp4"
                segment_path = work_dir / f"segment_{index}.mp4"
                textfile_path = _write_drawtext_textfile(work_dir, index, _section_text(section))
                _download_url(clip_url, clip_path)
                _prepare_video_segment(clip_path, segment_path, section_duration, textfile_path, job_key=job_key)
                segment_paths.append(segment_path)

            _persist_lifecycle(
                job_key, STATE_SEGMENT_PREP_DONE, ASSEMBLE_PROCESSING, job,
                current_step="concat",
                segment_count=len(segment_paths),
            )

            concat_video_path = work_dir / "concat_video.mp4"
            _concat_segments(segment_paths, concat_video_path, work_dir, job_key=job_key)

            music_path: Optional[Path] = None
            music_url = _pixabay_music_url(job_key=job_key)
            if music_url:
                candidate_music_path = work_dir / "music_audio"
                try:
                    _log_event("AUDIO_FETCH_START", job_key, source="music")
                    _download_url(music_url, candidate_music_path)
                    _log_event("AUDIO_FETCH_DONE", job_key, source="music", bytes=candidate_music_path.stat().st_size)
                    music_path = candidate_music_path
                except requests.RequestException as exc:
                    _log_event(
                        "MUSIC_FETCH_FAILED_NONFATAL",
                        job_key,
                        provider="pixabay_media",
                        error_message=_sanitize_external_error(exc),
                    )
                    _log_event("MUSIC_SKIPPED", job_key, reason="music_media_download_failed")

            _persist_lifecycle(
                job_key, STATE_FINAL_MUX_START, ASSEMBLE_PROCESSING, job,
                current_step="final_mux",
                mux_mode="with_music" if music_path else "voice_only",
            )
            _log_event("FINAL_MUX_START", job_key, mode="with_music" if music_path else "voice_only")
            _mix_audio(concat_video_path, voice_path, music_path, output_path, voice_duration, job_key=job_key)
            if not output_path.exists():
                raise RuntimeError("final mux completed without output file")
            _log_event(
                "FINAL_MUX_DONE",
                job_key,
                mode="with_music" if music_path else "voice_only",
                output_path=_safe_log_path(output_path),
                bytes=output_path.stat().st_size,
            )
            _persist_lifecycle(
                job_key, STATE_FINAL_MUX_DONE, ASSEMBLE_PROCESSING, job,
                current_step="artifact_upload",
                output_path=str(output_path),
                bytes_local=output_path.stat().st_size,
            )

        if not output_path.exists():
            raise RuntimeError("final output file missing before completion state write")

        # Mirror final artifact to Drive BEFORE writing terminal state, so a
        # restart immediately after STATE_COMPLETE_WRITTEN cannot lose it.
        drive_file_id = ""
        try:
            drive_file_id = state_store.upload_artifact(job_key, output_path)
        except Exception as exc:
            _log_event(
                "ARTIFACT_MIRROR_FAILED",
                job_key,
                error_message=_sanitize_external_error(exc),
            )
        if state_store.is_enabled() and not drive_file_id:
            raise ArtifactMirrorError("artifact upload to Drive failed")

        job["status"] = "done"
        job["output_path"] = str(output_path)
        job["output_video_url"] = f"/assemble-jobs/{job_key}/video"
        job["drive_file_id"] = drive_file_id
        job["updated_at"] = _now()
        _persist_lifecycle(
            job_key, STATE_COMPLETE_WRITTEN, ASSEMBLE_DONE, job,
            require_mirror=True,
            drive_file_id=drive_file_id,
        )
        _log_event(
            "STATE_COMPLETE_WRITTEN",
            job_key,
            output_video_url=job["output_video_url"],
            output_path=_safe_log_path(output_path),
            bytes=output_path.stat().st_size,
        )
        _job_path(ASSEMBLE_PROCESSING, job_key).unlink(missing_ok=True)
    except Exception as exc:
        job["status"] = "failed"
        if isinstance(exc, ArtifactMirrorError):
            job["error_class"] = "ARTIFACT_MIRROR"
            job["error_message"] = _sanitize_external_error(exc)[:500]
        elif isinstance(exc, StateMirrorError):
            job["error_class"] = "STATE_MIRROR"
            job["error_message"] = _sanitize_external_error(exc)[:500]
        else:
            job["error_class"] = "ASSEMBLY"
            job["error_message"] = _sanitize_external_error(exc)[:500]
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
        _persist_lifecycle(
            job_key, STATE_FAILED, ASSEMBLE_FAILED, job,
            error_class=job.get("error_class"),
            ffmpeg_returncode=job.get("ffmpeg_returncode"),
        )
        _job_path(ASSEMBLE_DONE, job_key).unlink(missing_ok=True)
        _job_path(ASSEMBLE_PROCESSING, job_key).unlink(missing_ok=True)


def register_assembly_routes(app: FastAPI) -> None:
    for folder in (ASSEMBLE_PROCESSING, ASSEMBLE_DONE, ASSEMBLE_FAILED, ASSEMBLE_STDERR, ASSEMBLE_OUTPUTS):
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
            "audio_url": body.audio_url,
            "script_sections": [
                (s.model_dump() if hasattr(s, "model_dump") else s.dict())
                for s in body.script_sections
            ],
            "status": "processing",
            "lifecycle_state": STATE_QUEUED,
            "current_step": "queued",
            "output_video_url": None,
            "output_path": None,
            "drive_file_id": None,
            "error_class": None,
            "error_message": None,
            "ffmpeg_returncode": None,
            "ffmpeg_stderr_path": None,
            "ffmpeg_stderr_tail": None,
            "received_at": _now(),
            "updated_at": _now(),
        }
        _save_job(ASSEMBLE_PROCESSING, job_key, job)

        # When persistence is enabled, persist QUEUED to Drive BEFORE returning
        # 202. If the mirror fails, reject the submission with 503 instead of
        # silently accepting a job that cannot survive a restart.
        if state_store.is_enabled():
            mirror_id = state_store.write_state(job_key, job)
            if not mirror_id:
                _job_path(ASSEMBLE_PROCESSING, job_key).unlink(missing_ok=True)
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error_class": "PERSISTENCE",
                        "error_message": "could not persist job state",
                    },
                )
            _log_event(
                f"LIFECYCLE_{STATE_QUEUED}",
                job_key,
                content_id=body.content_id,
                status="processing",
            )

        threading.Thread(target=_process_assemble_job, args=(job_key, body), daemon=True).start()
        # Response shape preserved verbatim for existing test contract:
        # set(data.keys()) == {"job_key", "status"} ; status == "processing".
        return {"job_key": job_key, "status": "processing"}

    @app.get("/assemble-jobs/{job_key}")
    def get_assemble_job(
        job_key: str,
        x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
    ):
        _check_auth(x_runner_auth)
        job = _find_job(job_key)
        if not job:
            raise HTTPException(
                status_code=404,
                detail={
                    "error_class": "NOT_FOUND",
                    "error_message": "assemble job not found",
                    "job_key": job_key,
                },
            )
        stderr_tail = None
        if job.get("status") == "failed":
            if "ffmpeg_stderr_tail" in job:
                stderr_tail = job.get("ffmpeg_stderr_tail")
            else:
                stderr_tail = _stderr_tail(job_key) or []
        output_path = Path(job["output_path"]) if job.get("output_path") else None
        return {
            "job_key": job["job_key"],
            "status": job["status"],
            "lifecycle_state": job.get("lifecycle_state"),
            "current_step": job.get("current_step"),
            "output_video_url": job.get("output_video_url"),
            "output_exists": bool(output_path and output_path.exists()),
            "drive_file_id": job.get("drive_file_id"),
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
        _log_event("VIDEO_DOWNLOAD_START", job_key)

        job = _find_job(job_key)
        if not job:
            # Truly unknown — never seen, no local file, no Drive mirror.
            _log_event("VIDEO_DOWNLOAD_NOT_FOUND", job_key, reason="job_unknown")
            raise HTTPException(
                status_code=404,
                detail={
                    "error_class": "NOT_FOUND",
                    "error_message": "assemble video not found",
                    "job_key": job_key,
                },
            )

        # Job exists but is not yet (or never will be) at terminal-success.
        # Do NOT silently 404 — return structured status the caller can act on.
        # Treat legacy records (status=="done" with no lifecycle_state) as complete
        # to preserve backward compatibility with pre-patch DONE-folder records.
        is_complete = (
            job.get("lifecycle_state") == STATE_COMPLETE_WRITTEN
            or (job.get("lifecycle_state") is None and job.get("status") == "done")
        )
        if not is_complete:
            _log_event(
                "VIDEO_DOWNLOAD_NOT_READY",
                job_key,
                lifecycle_state=job.get("lifecycle_state"),
                status=job.get("status"),
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "error_class": "NOT_READY",
                    "error_message": "assemble job not yet complete",
                    "job_key": job_key,
                    "status": job.get("status"),
                    "lifecycle_state": job.get("lifecycle_state"),
                    "error_class_inner": job.get("error_class"),
                    "error_message_inner": job.get("error_message"),
                },
            )

        # Terminal success state. Prefer the local file (fast path).
        local_path: Optional[Path] = (
            Path(job["output_path"]) if job.get("output_path") else None
        )
        if local_path and local_path.exists():
            size = local_path.stat().st_size
            _log_event(
                "VIDEO_DOWNLOAD_OK",
                job_key,
                output_path=_safe_log_path(local_path),
                bytes=size,
            )
            return FileResponse(
                local_path,
                media_type="video/mp4",
                filename=local_path.name,
            )

        # Local file gone (e.g. post-restart). If we have a Drive mirror id,
        # surface it so the caller can fetch the artifact from Drive directly.
        if job.get("drive_file_id"):
            _log_event(
                "VIDEO_DOWNLOAD_NOT_FOUND",
                job_key,
                reason="output_file_missing",
                output_path=_safe_log_path(local_path),
            )
            return JSONResponse(
                status_code=200,
                content={
                    "job_key": job_key,
                    "lifecycle_state": job.get("lifecycle_state"),
                    "drive_file_id": job["drive_file_id"],
                    "message": "local artifact missing post-restart; fetch from Drive",
                },
            )

        # Complete state, no local file, no Drive mirror id.
        # If persistence is enabled, this is a durability promise we failed to
        # keep → return 410 ARTIFACT_LOST. If persistence is disabled (legacy),
        # the old contract was a plain 404 for missing local file.
        _log_event(
            "VIDEO_DOWNLOAD_NOT_FOUND",
            job_key,
            reason="output_file_missing",
            output_path=_safe_log_path(local_path),
        )
        if state_store.is_enabled():
            raise HTTPException(
                status_code=410,
                detail={
                    "error_class": "ARTIFACT_LOST",
                    "error_message": "artifact not present locally or in Drive",
                    "job_key": job_key,
                    "lifecycle_state": job.get("lifecycle_state"),
                },
            )
        raise HTTPException(
            status_code=404,
            detail={
                "error_class": "NOT_FOUND",
                "error_message": "assemble video file missing",
                "job_key": job_key,
            },
        )

    # /assemble-jobs-health intentionally removed: app.py /health is now the
    # single authenticated endpoint that reports persisted-non-terminal counts.
    # Keeping a separate, unauthenticated route exposed job_key data.
