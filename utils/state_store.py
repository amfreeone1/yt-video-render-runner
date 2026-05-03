"""
Restart-safe state mirror for assembly jobs.

Mirrors job state JSON to a dedicated Google Drive folder so state
survives Render instance recycles. Local filesystem is treated as a
fast cache; Drive is the source of truth for cross-restart recovery.

Uses only primitives from utils.drive_upload:
  - upsert_json_state(folder_id, name, state) -> drive_file_id
  - read_json_state(folder_id, name) -> dict | None
  - list_json_state_files(folder_id, name_prefix) -> [{id, name}]
  - upload_file_to_folder(file_path, folder_id, ...) -> drive_file_id
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils import drive_upload

log = logging.getLogger("uvicorn.error")

import os

STATE_FOLDER_ENV = "ASSEMBLY_STATE_DRIVE_FOLDER_ID"
ARTIFACTS_FOLDER_ENV = "ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID"

STATE_NAME_PREFIX = "assembly_state_"
STATE_NAME_SUFFIX = ".json"


def _state_folder_id() -> str:
    return os.environ.get(STATE_FOLDER_ENV, "")


def _artifacts_folder_id() -> str:
    return os.environ.get(ARTIFACTS_FOLDER_ENV, "")


def is_enabled() -> bool:
    """
    True only when BOTH state and artifacts folder env vars are set.
    Partial config (state without artifacts, or vice-versa) disables the
    whole feature so we cannot reach a `done` state with a non-durable
    artifact.
    """
    return bool(_state_folder_id() and _artifacts_folder_id())


def state_filename(job_key: str) -> str:
    return f"{STATE_NAME_PREFIX}{job_key}{STATE_NAME_SUFFIX}"


def _job_key_from_filename(name: str) -> Optional[str]:
    if not name.startswith(STATE_NAME_PREFIX) or not name.endswith(STATE_NAME_SUFFIX):
        return None
    return name[len(STATE_NAME_PREFIX):-len(STATE_NAME_SUFFIX)]


def write_state(job_key: str, state: Dict[str, Any]) -> str:
    """
    Mirror a state record to Drive. Returns drive_file_id or "" on disable/failure.
    Failures are non-fatal — local state is still authoritative for the running worker.
    """
    if not is_enabled() or not job_key:
        return ""
    folder_id = _state_folder_id()
    payload = dict(state)
    payload["_mirror_ts"] = time.time()
    file_id = drive_upload.upsert_json_state(
        folder_id,
        state_filename(job_key),
        payload,
    )
    if not file_id:
        log.warning("state_store.write_state mirror failed job_key=%s", job_key)
    return file_id


def read_state(job_key: str) -> Optional[Dict[str, Any]]:
    """Read a state record from Drive. Returns None if missing or disabled."""
    if not is_enabled() or not job_key:
        return None
    folder_id = _state_folder_id()
    return drive_upload.read_json_state(folder_id, state_filename(job_key))


def list_all_states() -> List[Dict[str, Any]]:
    """Read every persisted state record. Used by startup scan."""
    if not is_enabled():
        return []
    folder_id = _state_folder_id()
    files = drive_upload.list_json_state_files(
        folder_id,
        name_prefix=STATE_NAME_PREFIX,
    )
    out: List[Dict[str, Any]] = []
    for f in files:
        name = f.get("name", "")
        job_key = _job_key_from_filename(name)
        if not job_key:
            continue
        state = drive_upload.read_json_state(folder_id, name)
        if state:
            out.append(state)
    return out


def upload_artifact(job_key: str, local_path: Path) -> str:
    """
    Upload final MP4 to the artifacts Drive folder.
    Returns drive_file_id or "" on disable/failure.
    """
    if not is_enabled() or not job_key:
        return ""
    folder_id = _artifacts_folder_id()
    if not local_path.exists():
        return ""
    return drive_upload.upload_file_to_folder(
        local_path,
        folder_id,
        drive_filename=f"{job_key}.mp4",
        mime_type="video/mp4",
    )
