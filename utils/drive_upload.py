import io
import json
import logging
import mimetypes
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload

log = logging.getLogger("uvicorn.error")

_OAUTH_TOKEN_URI = "https://oauth2.googleapis.com/token"


def _build_service():
    """
    Build a Drive v3 service from OAuth2 refresh-token env vars.
    Returns (service, ok). On missing creds, returns (None, False) and logs once.
    """
    refresh_token = os.environ.get("GOOGLE_DRIVE_REFRESH_TOKEN", "")
    client_id = os.environ.get("GOOGLE_DRIVE_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_DRIVE_CLIENT_SECRET", "")
    if not all([refresh_token, client_id, client_secret]):
        return None, False
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri=_OAUTH_TOKEN_URI,
        client_id=client_id,
        client_secret=client_secret,
    )
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service, True


def upload_file_to_drive(file_path: Path) -> str:
    """
    Upload rendered artifact to Google Drive via OAuth2 refresh token.
    Uses GOOGLE_DRIVE_FOLDER_ID env var as parent folder.
    Returns drive_file_id or "" on failure (non-fatal).

    Preserved from original to avoid breaking app.py callers.
    """
    try:
        folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
        service, ok = _build_service()
        if not ok or not folder_id:
            log.warning("Drive upload skipped: missing env vars")
            return ""

        media = MediaFileUpload(
            str(file_path),
            mimetype="video/mp4",
            resumable=True,
        )
        metadata = {
            "name": Path(file_path).name,
            "parents": [folder_id],
        }
        uploaded = service.files().create(
            body=metadata,
            media_body=media,
            fields="id,name",
        ).execute()
        file_id = uploaded.get("id", "")
        log.info("Drive upload done: %s -> %s", Path(file_path).name, file_id)
        return file_id
    except Exception as e:
        log.warning("Drive upload failed for %s: %s", file_path, e)
        return ""


def upload_file_to_folder(
    file_path: Path,
    folder_id: str,
    *,
    drive_filename: Optional[str] = None,
    mime_type: Optional[str] = None,
) -> str:
    """
    Upload an arbitrary file to a specified Drive folder with optional rename
    and explicit MIME. Returns drive_file_id or "" on failure (non-fatal).
    """
    try:
        if not folder_id:
            log.warning("Drive folder upload skipped: empty folder_id")
            return ""
        service, ok = _build_service()
        if not ok:
            log.warning("Drive folder upload skipped: missing OAuth env vars")
            return ""

        local_path = Path(file_path)
        name = drive_filename or local_path.name
        mt = mime_type or mimetypes.guess_type(name)[0] or "application/octet-stream"

        media = MediaFileUpload(str(local_path), mimetype=mt, resumable=True)
        metadata = {"name": name, "parents": [folder_id]}
        uploaded = service.files().create(
            body=metadata,
            media_body=media,
            fields="id,name",
        ).execute()
        file_id = uploaded.get("id", "")
        log.info("Drive folder upload done: %s -> %s", name, file_id)
        return file_id
    except Exception as e:
        log.warning("Drive folder upload failed for %s: %s", file_path, e)
        return ""


def upsert_json_state(folder_id: str, name: str, state: Dict[str, Any]) -> str:
    """
    Create-or-update a JSON state file by name in a folder.
    Returns drive_file_id or "" on failure (non-fatal).
    """
    try:
        if not folder_id or not name:
            return ""
        service, ok = _build_service()
        if not ok:
            return ""

        existing_id = _find_file_id_by_name(service, folder_id, name)
        body_bytes = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
        media = MediaIoBaseUpload(
            io.BytesIO(body_bytes),
            mimetype="application/json",
            resumable=False,
        )
        if existing_id:
            updated = service.files().update(
                fileId=existing_id,
                media_body=media,
                fields="id,name",
            ).execute()
            return updated.get("id", existing_id)

        metadata = {"name": name, "parents": [folder_id]}
        created = service.files().create(
            body=metadata,
            media_body=media,
            fields="id,name",
        ).execute()
        return created.get("id", "")
    except Exception as e:
        log.warning("Drive upsert_json_state failed for %s: %s", name, e)
        return ""


def read_json_state(folder_id: str, name: str) -> Optional[Dict[str, Any]]:
    """
    Read a JSON state file by name from a folder.
    Returns parsed dict or None on miss/failure (non-fatal).
    """
    try:
        if not folder_id or not name:
            return None
        service, ok = _build_service()
        if not ok:
            return None
        file_id = _find_file_id_by_name(service, folder_id, name)
        if not file_id:
            return None
        return _download_file_as_json(service, file_id)
    except Exception as e:
        log.warning("Drive read_json_state failed for %s: %s", name, e)
        return None


def list_json_state_files(
    folder_id: str,
    *,
    name_prefix: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    List JSON files in a folder, optionally filtered by name prefix.
    Returns list of {"id", "name"} dicts. Empty list on failure (non-fatal).
    """
    try:
        if not folder_id:
            return []
        service, ok = _build_service()
        if not ok:
            return []

        out: List[Dict[str, str]] = []
        page_token: Optional[str] = None
        # Note: Drive `q` does not have a stable "starts with" string operator
        # for `name`, so we filter prefix client-side after listing JSON files.
        q = (
            f"'{folder_id}' in parents and trashed = false "
            f"and mimeType = 'application/json'"
        )
        while True:
            resp = service.files().list(
                q=q,
                fields="nextPageToken, files(id,name)",
                pageSize=1000,
                pageToken=page_token,
            ).execute()
            for f in resp.get("files", []):
                name = f.get("name", "")
                if name_prefix and not name.startswith(name_prefix):
                    continue
                if not name.endswith(".json"):
                    continue
                out.append({"id": f.get("id", ""), "name": name})
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return out
    except Exception as e:
        log.warning("Drive list_json_state_files failed: %s", e)
        return []


def delete_drive_file(file_id: str) -> bool:
    """
    Delete a file by id. Returns True on success, False otherwise (non-fatal).
    """
    try:
        if not file_id:
            return False
        service, ok = _build_service()
        if not ok:
            return False
        service.files().delete(fileId=file_id).execute()
        return True
    except HttpError as e:
        log.warning("Drive delete_drive_file failed for %s: %s", file_id, e)
        return False
    except Exception as e:
        log.warning("Drive delete_drive_file error for %s: %s", file_id, e)
        return False


def _find_file_id_by_name(service, folder_id: str, name: str) -> str:
    """
    Find a single file id by exact name in a folder. Returns "" if not found.
    If multiple exist, returns the first; caller may dedupe via delete_drive_file.
    """
    try:
        safe_name = name.replace("'", "\\'")
        q = (
            f"'{folder_id}' in parents and trashed = false "
            f"and name = '{safe_name}'"
        )
        resp = service.files().list(
            q=q,
            fields="files(id,name)",
            pageSize=10,
        ).execute()
        files = resp.get("files", [])
        if not files:
            return ""
        return files[0].get("id", "")
    except Exception as e:
        log.warning("Drive _find_file_id_by_name error for %s: %s", name, e)
        return ""


def _download_file_as_json(service, file_id: str) -> Optional[Dict[str, Any]]:
    """Download a Drive file by id and parse as JSON. None on any failure."""
    try:
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _status, done = downloader.next_chunk()
        return json.loads(buf.getvalue().decode("utf-8"))
    except Exception as e:
        log.warning("Drive _download_file_as_json error for %s: %s", file_id, e)
        return None
