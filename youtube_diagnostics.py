"""
YouTube diagnostic routes — read-only.

Adds GET /youtube/channel-identity.
Uses existing YOUTUBE_CLIENT_ID / YOUTUBE_CLIENT_SECRET / YOUTUBE_REFRESH_TOKEN
env vars (same credentials as /upload-jobs) to identify which YouTube channel
the current token is authorised for.

No uploads, edits, deletions, publishes, or schedule changes are performed.
No tokens, secrets, refresh tokens, or credential objects are returned or logged.
"""

import json
import logging
import os
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

log = logging.getLogger("uvicorn.error")

_TOKEN_URI = "https://oauth2.googleapis.com/token"

# Scopes the token is expected to carry.  Read-only is sufficient for this
# diagnostic; we list youtube.upload as well because the existing token was
# issued with that scope and we do not want a scope-mismatch to cause a
# silent token refresh to a narrower credential set.
_EXPECTED_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_auth(x_runner_auth: Optional[str]) -> None:
    """Reject requests that do not supply the correct runner auth token."""
    expected = os.environ.get("RUNNER_AUTH_TOKEN")
    if not expected or x_runner_auth != expected:
        raise HTTPException(
            status_code=401,
            detail={"error_class": "AUTH", "error_message": "Unauthorized"},
        )


def _build_youtube_client():
    """
    Build a YouTube Data API v3 client from existing OAuth env vars.
    Raises HTTPException(503) if any required var is absent.
    Never logs or returns credential values.
    """
    client_id = os.environ.get("YOUTUBE_CLIENT_ID", "")
    client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET", "")
    refresh_token = os.environ.get("YOUTUBE_REFRESH_TOKEN", "")

    missing = [
        name
        for name, val in [
            ("YOUTUBE_CLIENT_ID", client_id),
            ("YOUTUBE_CLIENT_SECRET", client_secret),
            ("YOUTUBE_REFRESH_TOKEN", refresh_token),
        ]
        if not val
    ]
    if missing:
        raise HTTPException(
            status_code=503,
            detail={
                "error_class": "CONFIG",
                "error_message": f"missing env vars: {', '.join(missing)}",
            },
        )

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri=_TOKEN_URI,
        client_id=client_id,
        client_secret=client_secret,
        scopes=_EXPECTED_SCOPES,
    )
    return build("youtube", "v3", credentials=creds, cache_discovery=False)


def _classify_channels_list_error(e: HttpError) -> JSONResponse:
    """
    Map a channels.list HttpError to a safe JSONResponse.
    No raw API error body, token detail, or credential value is forwarded.
    """
    status = e.resp.status
    try:
        details = json.loads(e.content.decode("utf-8")).get("error", {})
        reasons = [err.get("reason", "") for err in details.get("errors", [])]
    except Exception:
        reasons = []

    auth_reasons = {
        "authError",
        "forbidden",
        "insufficientPermissions",
        "unauthorized",
        "invalidCredentials",
    }
    if status in (401, 403) or auth_reasons.intersection(reasons):
        return JSONResponse(
            status_code=403,
            content={
                "error_class": "AUTH_SCOPE",
                "error_message": (
                    "regenerate refresh token with "
                    "youtube.upload + youtube.readonly scopes"
                ),
            },
        )

    # Any other API error — surface HTTP status only, no raw message.
    return JSONResponse(
        status_code=502,
        content={
            "error_class": "YOUTUBE_API",
            "error_message": f"channels.list returned HTTP {status}",
        },
    )


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------

def register_youtube_diagnostic_routes(app: FastAPI) -> None:

    @app.get("/youtube/channel-identity")
    def channel_identity(
        x_runner_auth: Optional[str] = Header(None, alias="X-Runner-Auth"),
    ):
        """
        Read-only diagnostic: identify the YouTube channel the current
        YOUTUBE_REFRESH_TOKEN is authorised for.

        Returns a safe subset of channels.list fields only:
          channel_id, title, custom_url, description (first 120 chars),
          uploads_playlist_id, subscriber_count, video_count,
          privacy_status, is_linked, long_uploads_status.

        No token, email, secret, credential object, or raw OAuth response
        is returned or logged.
        """
        _check_auth(x_runner_auth)

        # Build client — raises HTTPException on missing config.
        try:
            client = _build_youtube_client()
        except HTTPException:
            raise
        except Exception:
            log.exception("channel-identity: YouTube client build failed")
            raise HTTPException(
                status_code=500,
                detail={
                    "error_class": "CLIENT_BUILD",
                    "error_message": "failed to build YouTube client",
                },
            )

        # Call channels.list — read-only, no side effects.
        try:
            response = (
                client.channels()
                .list(
                    part="snippet,contentDetails,statistics,status",
                    mine=True,
                )
                .execute()
            )
        except HttpError as e:
            return _classify_channels_list_error(e)
        except Exception:
            log.exception("channel-identity: channels.list request failed")
            raise HTTPException(
                status_code=500,
                detail={
                    "error_class": "INTERNAL",
                    "error_message": "channels.list request failed",
                },
            )

        items = response.get("items", [])
        if not items:
            return JSONResponse(
                status_code=404,
                content={
                    "error_class": "NO_CHANNEL",
                    "error_message": (
                        "no YouTube channel found for this token; "
                        "ensure the OAuth consent was granted by the channel owner"
                    ),
                },
            )

        channel = items[0]
        snippet = channel.get("snippet", {})
        content_details = channel.get("contentDetails", {})
        statistics = channel.get("statistics", {})
        status_obj = channel.get("status", {})

        # --- Safe identity fields only ---
        # Deliberately omit: email, brandingSettings, localizations,
        # auditDetails, contentOwnerDetails, invideoPromotion, raw token fields.
        description_raw = snippet.get("description") or ""
        result = {
            "channel_id": channel.get("id") or "",
            "title": snippet.get("title") or "",
            "custom_url": snippet.get("customUrl") or None,
            "description": description_raw[:120] or None,
            "uploads_playlist_id": (
                content_details
                .get("relatedPlaylists", {})
                .get("uploads")
            ) or None,
            "subscriber_count": statistics.get("subscriberCount") or None,
            "video_count": statistics.get("videoCount") or None,
            "privacy_status": status_obj.get("privacyStatus") or None,
            "is_linked": status_obj.get("isLinked"),
            "long_uploads_status": status_obj.get("longUploadsStatus") or None,
        }

        return JSONResponse(status_code=200, content=result)
