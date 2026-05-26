"""
Smoke tests for GET /youtube/channel-identity.

All tests use mocked YouTube API responses — no real network calls,
no real credentials, no secrets logged or asserted.
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

import youtube_diagnostics


def _make_client(fake_channels_list_response: dict):
    """Build a mock YouTube client whose channels().list().execute() returns the given dict."""
    mock_request = MagicMock()
    mock_request.execute.return_value = fake_channels_list_response
    mock_channels = MagicMock()
    mock_channels.list.return_value = mock_request
    mock_client = MagicMock()
    mock_client.channels.return_value = mock_channels
    return mock_client


def _full_channel_response(overrides: dict = None) -> dict:
    base = {
        "items": [
            {
                "id": "UC_test_channel_id",
                "snippet": {
                    "title": "Test Channel",
                    "customUrl": "@testchannel",
                    "description": "A" * 200,  # longer than 120 — should be truncated
                },
                "contentDetails": {
                    "relatedPlaylists": {
                        "uploads": "UU_test_uploads_playlist",
                    }
                },
                "statistics": {
                    "subscriberCount": "42000",
                    "videoCount": "17",
                },
                "status": {
                    "privacyStatus": "public",
                    "isLinked": True,
                    "longUploadsStatus": "allowed",
                },
            }
        ]
    }
    if overrides:
        base.update(overrides)
    return base


class ChannelIdentityTests(unittest.TestCase):
    def setUp(self):
        self.original_token = os.environ.get("RUNNER_AUTH_TOKEN")
        self.original_client_id = os.environ.get("YOUTUBE_CLIENT_ID")
        self.original_client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET")
        self.original_refresh_token = os.environ.get("YOUTUBE_REFRESH_TOKEN")

        os.environ["RUNNER_AUTH_TOKEN"] = "test-runner-token"
        os.environ["YOUTUBE_CLIENT_ID"] = "test-client-id"
        os.environ["YOUTUBE_CLIENT_SECRET"] = "test-client-secret"
        os.environ["YOUTUBE_REFRESH_TOKEN"] = "test-refresh-token"

        self.addCleanup(self._restore_env)

        app = FastAPI()
        youtube_diagnostics.register_youtube_diagnostic_routes(app)
        self.client = TestClient(app)
        self.headers = {"X-Runner-Auth": "test-runner-token"}

    def _restore_env(self):
        for name, original in [
            ("RUNNER_AUTH_TOKEN", self.original_token),
            ("YOUTUBE_CLIENT_ID", self.original_client_id),
            ("YOUTUBE_CLIENT_SECRET", self.original_client_secret),
            ("YOUTUBE_REFRESH_TOKEN", self.original_refresh_token),
        ]:
            if original is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = original

    # ── Auth guard ───────────────────────────────────────────────────────────

    def test_missing_auth_returns_401(self):
        response = self.client.get("/youtube/channel-identity")
        self.assertEqual(response.status_code, 401)

    def test_wrong_auth_returns_401(self):
        response = self.client.get(
            "/youtube/channel-identity",
            headers={"X-Runner-Auth": "wrong-token"},
        )
        self.assertEqual(response.status_code, 401)

    # ── Config guard ─────────────────────────────────────────────────────────

    def test_missing_youtube_env_vars_returns_503(self):
        del os.environ["YOUTUBE_REFRESH_TOKEN"]
        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          wraps=youtube_diagnostics._build_youtube_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )
        self.assertEqual(response.status_code, 503)
        # FastAPI wraps HTTPException payloads under "detail"
        body = response.json().get("detail", response.json())
        self.assertEqual(body["error_class"], "CONFIG")
        self.assertIn("YOUTUBE_REFRESH_TOKEN", body["error_message"])

    # ── Happy path ───────────────────────────────────────────────────────────

    def test_returns_safe_identity_fields(self):
        mock_client = _make_client(_full_channel_response())
        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          return_value=mock_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["channel_id"], "UC_test_channel_id")
        self.assertEqual(body["title"], "Test Channel")
        self.assertEqual(body["custom_url"], "@testchannel")
        self.assertEqual(body["uploads_playlist_id"], "UU_test_uploads_playlist")
        self.assertEqual(body["subscriber_count"], "42000")
        self.assertEqual(body["video_count"], "17")
        self.assertEqual(body["privacy_status"], "public")
        self.assertTrue(body["is_linked"])
        self.assertEqual(body["long_uploads_status"], "allowed")

    def test_description_is_truncated_to_120_chars(self):
        mock_client = _make_client(_full_channel_response())
        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          return_value=mock_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )
        self.assertEqual(response.status_code, 200)
        desc = response.json()["description"]
        self.assertIsNotNone(desc)
        self.assertLessEqual(len(desc), 120)

    # ── Secret leak guards ───────────────────────────────────────────────────

    def test_response_contains_no_token_values(self):
        """No credential string from env vars appears anywhere in the response body."""
        mock_client = _make_client(_full_channel_response())
        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          return_value=mock_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )
        raw = response.text
        for secret_value in [
            "test-client-id",
            "test-client-secret",
            "test-refresh-token",
            "test-runner-token",
        ]:
            self.assertNotIn(
                secret_value, raw,
                msg=f"Secret value '{secret_value}' must not appear in response",
            )

    def test_response_keys_do_not_include_credential_fields(self):
        mock_client = _make_client(_full_channel_response())
        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          return_value=mock_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )
        body = response.json()
        forbidden_keys = {
            "token", "refresh_token", "access_token", "client_secret",
            "client_id", "email", "credentials", "raw_response",
        }
        self.assertFalse(
            forbidden_keys.intersection(body.keys()),
            msg=f"Response must not include credential keys; found: "
                f"{forbidden_keys.intersection(body.keys())}",
        )

    # ── Edge cases ───────────────────────────────────────────────────────────

    def test_no_channel_returns_404(self):
        mock_client = _make_client({"items": []})
        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          return_value=mock_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error_class"], "NO_CHANNEL")

    def test_optional_fields_absent_returns_none(self):
        """Channels without customUrl or statistics should still return 200."""
        sparse = {
            "items": [
                {
                    "id": "UC_sparse",
                    "snippet": {"title": "Sparse Channel"},
                    "contentDetails": {},
                    "statistics": {},
                    "status": {},
                }
            ]
        }
        mock_client = _make_client(sparse)
        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          return_value=mock_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["channel_id"], "UC_sparse")
        self.assertIsNone(body["custom_url"])
        self.assertIsNone(body["subscriber_count"])
        self.assertIsNone(body["uploads_playlist_id"])

    # ── Auth-scope error ─────────────────────────────────────────────────────

    def test_auth_scope_error_returns_403_with_safe_message(self):
        from googleapiclient.errors import HttpError

        mock_error_content = json.dumps({
            "error": {
                "code": 403,
                "message": "The caller does not have permission",
                "errors": [{"reason": "forbidden"}],
            }
        }).encode("utf-8")
        mock_resp = MagicMock()
        mock_resp.status = 403
        http_error = HttpError(resp=mock_resp, content=mock_error_content)

        mock_request = MagicMock()
        mock_request.execute.side_effect = http_error
        mock_channels = MagicMock()
        mock_channels.list.return_value = mock_request
        mock_client = MagicMock()
        mock_client.channels.return_value = mock_channels

        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          return_value=mock_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )

        self.assertEqual(response.status_code, 403)
        body = response.json()
        self.assertEqual(body["error_class"], "AUTH_SCOPE")
        self.assertIn("youtube.readonly", body["error_message"])
        self.assertNotIn("The caller does not have permission", body["error_message"])

    def test_auth_scope_error_response_contains_no_secret_values(self):
        from googleapiclient.errors import HttpError

        mock_resp = MagicMock()
        mock_resp.status = 401
        http_error = HttpError(
            resp=mock_resp,
            content=json.dumps({"error": {"errors": [{"reason": "authError"}]}}).encode(),
        )
        mock_request = MagicMock()
        mock_request.execute.side_effect = http_error
        mock_channels = MagicMock()
        mock_channels.list.return_value = mock_request
        mock_client = MagicMock()
        mock_client.channels.return_value = mock_channels

        with patch.object(youtube_diagnostics, "_build_youtube_client",
                          return_value=mock_client):
            response = self.client.get(
                "/youtube/channel-identity", headers=self.headers
            )

        raw = response.text
        for secret_value in [
            "test-client-id",
            "test-client-secret",
            "test-refresh-token",
        ]:
            self.assertNotIn(secret_value, raw)


if __name__ == "__main__":
    unittest.main()
