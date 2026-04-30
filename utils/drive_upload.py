import os
import logging
from pathlib import Path
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

log = logging.getLogger("uvicorn.error")

def upload_file_to_drive(file_path: Path) -> str:
    """
    Upload rendered artifact to Google Drive via OAuth2 refresh token.
    Returns drive_file_id or "" on failure (non-fatal).
    """
    try:
        refresh_token = os.environ.get("GOOGLE_DRIVE_REFRESH_TOKEN", "")
        client_id = os.environ.get("GOOGLE_DRIVE_CLIENT_ID", "")
        client_secret = os.environ.get("GOOGLE_DRIVE_CLIENT_SECRET", "")
        folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

        if not all([refresh_token, client_id, client_secret, folder_id]):
            log.warning("Drive upload skipped: missing env vars")
            return ""

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
        )
        service = build("drive", "v3", credentials=creds)

        media = MediaFileUpload(
            str(file_path),
            mimetype="video/mp4",
            resumable=True
        )
        metadata = {
            "name": file_path.name,
            "parents": [folder_id],
        }
        uploaded = service.files().create(
            body=metadata,
            media_body=media,
            fields="id,name",
        ).execute()

        file_id = uploaded.get("id", "")
        log.info("Drive upload done: %s -> %s", file_path.name, file_id)
        return file_id

    except Exception as e:
        log.warning("Drive upload failed for %s: %s", file_path, e)
        return ""
