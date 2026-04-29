import json
import logging
import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
log = logging.getLogger("uvicorn.error")


def upload_file_to_drive(file_path: Path) -> str:
    """Upload a rendered artifact to Google Drive and return the Drive file ID.

    Failure is non-fatal for render completion: log a warning and return an empty
    string so callers can persist drive_upload_status without failing the job.
    """
    try:
        service_account_json = os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON", "")
        folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
        if not service_account_json or not folder_id:
            log.warning("Drive upload skipped: Google Drive env vars are missing")
            return ""

        credentials_info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=[DRIVE_SCOPE],
        )
        service = build("drive", "v3", credentials=credentials)
        media = MediaFileUpload(str(file_path), mimetype="video/mp4", resumable=True)
        metadata = {
            "name": file_path.name,
            "parents": [folder_id],
        }
        uploaded_file = (
            service.files()
            .create(
                body=metadata,
                media_body=media,
                fields="id,name",
            )
            .execute()
        )
        return uploaded_file.get("id", "")
    except Exception as e:
        log.warning("Drive upload failed for %s: %s", file_path, e)
        return ""
