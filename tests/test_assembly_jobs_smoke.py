import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

import assembly_jobs


class AssemblyJobsSmokeTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

        self.original_paths = {
            "JOBS": assembly_jobs.JOBS,
            "ASSEMBLE_JOBS": assembly_jobs.ASSEMBLE_JOBS,
            "ASSEMBLE_DONE": assembly_jobs.ASSEMBLE_DONE,
            "ASSEMBLE_FAILED": assembly_jobs.ASSEMBLE_FAILED,
            "ASSEMBLE_PROCESSING": assembly_jobs.ASSEMBLE_PROCESSING,
            "ASSEMBLE_STDERR": assembly_jobs.ASSEMBLE_STDERR,
        }
        self.addCleanup(self._restore_paths)

        assembly_jobs.JOBS = self.root / "jobs"
        assembly_jobs.ASSEMBLE_JOBS = assembly_jobs.JOBS / "assemble"
        assembly_jobs.ASSEMBLE_DONE = assembly_jobs.ASSEMBLE_JOBS / "done"
        assembly_jobs.ASSEMBLE_FAILED = assembly_jobs.ASSEMBLE_JOBS / "failed"
        assembly_jobs.ASSEMBLE_PROCESSING = assembly_jobs.ASSEMBLE_JOBS / "processing"
        assembly_jobs.ASSEMBLE_STDERR = assembly_jobs.ASSEMBLE_JOBS / "stderr"

        self.original_token = os.environ.get("RUNNER" + "_AUTH_TOKEN")
        os.environ["RUNNER" + "_AUTH_TOKEN"] = "smoke-token"
        self.original_pixabay_key = os.environ.get("PIXABAY" + "_API_KEY")
        os.environ["PIXABAY" + "_API_KEY"] = "pixabay-secret-key"
        self.addCleanup(self._restore_token)
        self.addCleanup(self._restore_pixabay_key)

        app = FastAPI()
        assembly_jobs.register_assembly_routes(app)
        self.client = TestClient(app)
        self.headers = {"X-Runner-Auth": "smoke-token"}

    def _restore_paths(self):
        for name, value in self.original_paths.items():
            setattr(assembly_jobs, name, value)

    def _restore_token(self):
        if self.original_token is None:
            os.environ.pop("RUNNER" + "_AUTH_TOKEN", None)
        else:
            os.environ["RUNNER" + "_AUTH_TOKEN"] = self.original_token

    def _restore_pixabay_key(self):
        if self.original_pixabay_key is None:
            os.environ.pop("PIXABAY" + "_API_KEY", None)
        else:
            os.environ["PIXABAY" + "_API_KEY"] = self.original_pixabay_key

    def test_assemble_job_accepted_response_contract(self):
        payload = {
            "content_id": "YT-20260427-02-r1",
            "audio_url": "https://example.test/audio.mp3",
            "script_sections": [{"section": "HOOK", "text": "Smoke section"}],
        }
        with patch.object(assembly_jobs, "_process_assemble_job", lambda *args, **kwargs: None):
            response = self.client.post("/assemble-job", json=payload, headers=self.headers)

        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertEqual(set(data.keys()), {"job_key", "status"})
        self.assertEqual(data["status"], "processing")
        self.assertTrue(data["job_key"].startswith("assemble_YT-20260427-02-r1_"))

    def test_failed_status_includes_returncode_and_last_80_stderr_lines(self):
        job_key = "assemble_smoke_failed"
        assembly_jobs._save_job(
            assembly_jobs.ASSEMBLE_FAILED,
            job_key,
            {
                "job_key": job_key,
                "status": "failed",
                "output_video_url": None,
                "error_class": "ASSEMBLY",
                "error_message": "boom",
                "ffmpeg_returncode": 1,
                "updated_at": "2026-05-02T00:00:00Z",
            },
        )
        assembly_jobs.ASSEMBLE_STDERR.mkdir(parents=True, exist_ok=True)
        assembly_jobs._stderr_path(job_key).write_text(
            "\n".join(f"line {index}" for index in range(1, 101)),
            encoding="utf-8",
        )

        response = self.client.get(f"/assemble-jobs/{job_key}", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "failed")
        self.assertEqual(data["error_class"], "ASSEMBLY")
        self.assertEqual(data["ffmpeg_returncode"], 1)
        self.assertEqual(len(data["ffmpeg_stderr_tail"]), 80)
        self.assertEqual(data["ffmpeg_stderr_tail"][0], "line 21")
        self.assertEqual(data["ffmpeg_stderr_tail"][-1], "line 100")
        self.assertEqual(data["updated_at"], "2026-05-02T00:00:00Z")

    def test_successful_status_does_not_return_stderr_tail(self):
        job_key = "assemble_smoke_done"
        assembly_jobs._save_job(
            assembly_jobs.ASSEMBLE_DONE,
            job_key,
            {
                "job_key": job_key,
                "status": "done",
                "output_video_url": f"/assemble-jobs/{job_key}/video",
                "error_class": None,
                "error_message": None,
                "updated_at": "2026-05-02T00:00:00Z",
            },
        )
        assembly_jobs.ASSEMBLE_STDERR.mkdir(parents=True, exist_ok=True)
        assembly_jobs._stderr_path(job_key).write_text("successful ffmpeg stderr\n", encoding="utf-8")

        response = self.client.get(f"/assemble-jobs/{job_key}", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "done")
        self.assertIsNone(data["ffmpeg_stderr_tail"])

    def test_run_failure_preserves_returncode_and_per_job_stderr(self):
        job_key = "assemble_smoke_run_failure"
        command = [
            sys.executable,
            "-c",
            "import sys; sys.stderr.write('diagnostic stderr line one\\nroot cause line two\\n'); sys.exit(7)",
        ]

        with self.assertRaises(assembly_jobs.FFmpegCommandError) as raised:
            assembly_jobs._run(command, job_key=job_key, event="smoke_failure")

        self.assertEqual(raised.exception.returncode, 7)
        stderr_path = assembly_jobs._stderr_path(job_key)
        self.assertTrue(stderr_path.exists())
        stderr_content = stderr_path.read_text(encoding="utf-8")
        self.assertIn("diagnostic stderr line one", stderr_content)
        self.assertIn("root cause line two", stderr_content)
        self.assertNotEqual(stderr_content.strip(), "ffmpeg version")

    def test_drawtext_uses_textfile_for_filtergraph_unsafe_text(self):
        text = "HOOK: \"What if I told you that it isn't your washing machine, your TV, or even your air conditioner?\" — 100% real"
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp)
            textfile_path = assembly_jobs._write_drawtext_textfile(work_dir, 0, text)
            captured = {}

            def fake_run(cmd, *, job_key=None, event=None):
                captured["cmd"] = cmd
                captured["job_key"] = job_key
                captured["event"] = event
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with patch.object(assembly_jobs, "_run", fake_run):
                assembly_jobs._prepare_video_segment(
                    Path("/tmp/input.mp4"),
                    Path("/tmp/output.mp4"),
                    1.0,
                    textfile_path,
                    job_key="assemble_textfile_smoke",
                )

            vf = captured["cmd"][captured["cmd"].index("-vf") + 1]
            self.assertIn("drawtext=", vf)
            self.assertIn("textfile=", vf)
            self.assertNotIn(":text=", vf)
            self.assertNotIn(text, vf)
            self.assertEqual(textfile_path.read_text(encoding="utf-8"), text)
            self.assertEqual(captured["job_key"], "assemble_textfile_smoke")
            self.assertEqual(captured["event"], "prepare_video_segment")

    def test_prepare_video_segment_uses_720p_thread_limited_textfile_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp)
            textfile_path = assembly_jobs._write_drawtext_textfile(work_dir, 0, "HOOK: safe text")
            captured = {}

            def fake_run(cmd, *, job_key=None, event=None):
                captured["cmd"] = cmd
                captured["job_key"] = job_key
                captured["event"] = event
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with patch.object(assembly_jobs, "_run", fake_run):
                assembly_jobs._prepare_video_segment(
                    Path("/tmp/input.mp4"),
                    work_dir / "output.mp4",
                    1.0,
                    textfile_path,
                    job_key="assemble_memory_smoke",
                )

            cmd = captured["cmd"]
            vf = cmd[cmd.index("-vf") + 1]
            self.assertIn("scale=1280:720:force_original_aspect_ratio=increase", vf)
            self.assertIn("crop=1280:720", vf)
            self.assertIn("textfile=", vf)
            self.assertNotIn(":text=", vf)
            self.assertIn("-threads", cmd)
            thread_values = [cmd[index + 1] for index, value in enumerate(cmd[:-1]) if value == "-threads"]
            self.assertTrue(thread_values)
            self.assertTrue(all(value == "1" for value in thread_values))
            self.assertEqual(captured["job_key"], "assemble_memory_smoke")
            self.assertEqual(captured["event"], "prepare_video_segment")

    def test_pixabay_music_404_is_nonfatal_and_sanitized(self):
        response = MagicMock()
        response.raise_for_status.side_effect = requests.HTTPError(
            "404 Client Error: Not Found for url: https://pixabay.com/api/music/?key=pixabay-secret-key&q=cinematic+ambient"
        )
        events = []

        with patch.object(assembly_jobs.requests, "get", return_value=response), patch.object(assembly_jobs, "_log_event", lambda event, job_key, **fields: events.append((event, fields))):
            music_url = assembly_jobs._pixabay_music_url(job_key="assemble_music_404")

        self.assertIsNone(music_url)
        event_names = [event for event, _fields in events]
        self.assertIn("MUSIC_FETCH_START", event_names)
        self.assertIn("MUSIC_FETCH_FAILED_NONFATAL", event_names)
        self.assertIn("MUSIC_SKIPPED", event_names)
        failed = [fields for event, fields in events if event == "MUSIC_FETCH_FAILED_NONFATAL"][0]
        self.assertNotIn("pixabay-secret-key", failed["error_message"])
        self.assertIn("key=<redacted>", failed["error_message"])

    def test_assemble_continues_voice_only_when_music_unavailable(self):
        job_key = "assemble_music_optional"
        assembly_jobs._save_job(
            assembly_jobs.ASSEMBLE_PROCESSING,
            job_key,
            {
                "job_key": job_key,
                "content_id": "YT-20260427-02-r1",
                "status": "processing",
                "output_video_url": None,
                "output_path": None,
                "error_class": None,
                "error_message": None,
                "ffmpeg_returncode": None,
                "ffmpeg_stderr_path": None,
                "ffmpeg_stderr_tail": None,
                "received_at": "2026-05-02T00:00:00Z",
                "updated_at": "2026-05-02T00:00:00Z",
            },
        )
        body = assembly_jobs.AssembleJobRequest(
            content_id="YT-20260427-02-r1",
            audio_url="https://example.test/audio.mp3",
            script_sections=[assembly_jobs.ScriptSection(section="HOOK", text="Smoke section")],
        )
        events = []
        mix_calls = []

        def fake_download(_url, dest, **_kwargs):
            dest.write_bytes(b"media")

        def fake_prepare(_input_path, output_path, _duration, _textfile_path, *, job_key):
            output_path.write_bytes(b"segment")

        def fake_concat(_segment_paths, output_path, _work_dir, *, job_key):
            output_path.write_bytes(b"concat")

        def fake_mix(_video_path, _voice_path, music_path, output_path, _duration, *, job_key):
            mix_calls.append(music_path)
            output_path.write_bytes(b"final")

        with patch.object(assembly_jobs, "_download_url", fake_download), \
            patch.object(assembly_jobs, "_probe_duration", return_value=5.0), \
            patch.object(assembly_jobs, "_pexels_video_url", return_value="https://example.test/clip.mp4"), \
            patch.object(assembly_jobs, "_prepare_video_segment", fake_prepare), \
            patch.object(assembly_jobs, "_concat_segments", fake_concat), \
            patch.object(assembly_jobs, "_pixabay_music_url", return_value=None), \
            patch.object(assembly_jobs, "_mix_audio", fake_mix), \
            patch.object(assembly_jobs, "_log_event", lambda event, job_key, **fields: events.append((event, fields))):
            assembly_jobs._process_assemble_job(job_key, body)

        done_job = assembly_jobs._load_job(assembly_jobs.ASSEMBLE_DONE, job_key)
        failed_job = assembly_jobs._load_job(assembly_jobs.ASSEMBLE_FAILED, job_key)
        self.assertIsNotNone(done_job)
        self.assertIsNone(failed_job)
        self.assertEqual(done_job["status"], "done")
        self.assertEqual(mix_calls, [None])
        event_names = [event for event, _fields in events]
        self.assertIn("FINAL_MUX_START", event_names)
        self.assertIn("FINAL_MUX_DONE", event_names)
        self.assertIn("STATE_COMPLETE_WRITTEN", event_names)
        mux_start = [fields for event, fields in events if event == "FINAL_MUX_START"][0]
        self.assertEqual(mux_start["mode"], "voice_only")

    def test_failed_error_message_sanitizes_external_secrets(self):
        job_key = "assemble_sanitize_failure"
        assembly_jobs._save_job(
            assembly_jobs.ASSEMBLE_PROCESSING,
            job_key,
            {
                "job_key": job_key,
                "content_id": "YT-20260427-02-r1",
                "status": "processing",
                "output_video_url": None,
                "output_path": None,
                "error_class": None,
                "error_message": None,
                "ffmpeg_returncode": None,
                "ffmpeg_stderr_path": None,
                "ffmpeg_stderr_tail": None,
                "received_at": "2026-05-02T00:00:00Z",
                "updated_at": "2026-05-02T00:00:00Z",
            },
        )
        body = assembly_jobs.AssembleJobRequest(
            content_id="YT-20260427-02-r1",
            audio_url="https://example.test/audio.mp3",
            script_sections=[assembly_jobs.ScriptSection(section="HOOK", text="Smoke section")],
        )

        with patch.object(assembly_jobs, "_download_url", side_effect=requests.HTTPError("403 Client Error: Forbidden for url: https://example.test/audio.mp3?key=secret-token")):
            assembly_jobs._process_assemble_job(job_key, body)

        failed_job = assembly_jobs._load_job(assembly_jobs.ASSEMBLE_FAILED, job_key)
        self.assertIsNotNone(failed_job)
        self.assertEqual(failed_job["status"], "failed")
        self.assertNotIn("secret-token", failed_job["error_message"])
        self.assertIn("key=<redacted>", failed_job["error_message"])


if __name__ == "__main__":
    unittest.main()
