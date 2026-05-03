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
            "ASSEMBLE_OUTPUTS": assembly_jobs.ASSEMBLE_OUTPUTS,
        }
        self.addCleanup(self._restore_paths)

        assembly_jobs.JOBS = self.root / "jobs"
        assembly_jobs.ASSEMBLE_JOBS = assembly_jobs.JOBS / "assemble"
        assembly_jobs.ASSEMBLE_DONE = assembly_jobs.ASSEMBLE_JOBS / "done"
        assembly_jobs.ASSEMBLE_FAILED = assembly_jobs.ASSEMBLE_JOBS / "failed"
        assembly_jobs.ASSEMBLE_PROCESSING = assembly_jobs.ASSEMBLE_JOBS / "processing"
        assembly_jobs.ASSEMBLE_STDERR = assembly_jobs.ASSEMBLE_JOBS / "stderr"
        assembly_jobs.ASSEMBLE_OUTPUTS = assembly_jobs.ASSEMBLE_JOBS / "outputs"

        self.original_token = os.environ.get("RUNNER" + "_AUTH_TOKEN")
        os.environ["RUNNER" + "_AUTH_TOKEN"] = "smoke-token"
        self.original_pixabay_key = os.environ.get("PIXABAY" + "_API_KEY")
        os.environ["PIXABAY" + "_API_KEY"] = "pixabay-secret-key"
        # Persistence env: default to OFF for legacy tests; new tests opt in.
        self.original_state_folder = os.environ.get("ASSEMBLY_STATE_DRIVE_FOLDER_ID")
        if "ASSEMBLY_STATE_DRIVE_FOLDER_ID" in os.environ:
            del os.environ["ASSEMBLY_STATE_DRIVE_FOLDER_ID"]
        self.original_artifacts_folder = os.environ.get("ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID")
        if "ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID" in os.environ:
            del os.environ["ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID"]
        self.addCleanup(self._restore_token)
        self.addCleanup(self._restore_pixabay_key)
        self.addCleanup(self._restore_state_folder)
        self.addCleanup(self._restore_artifacts_folder)

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

    def _restore_state_folder(self):
        if self.original_state_folder is None:
            os.environ.pop("ASSEMBLY_STATE_DRIVE_FOLDER_ID", None)
        else:
            os.environ["ASSEMBLY_STATE_DRIVE_FOLDER_ID"] = self.original_state_folder

    def _restore_artifacts_folder(self):
        if self.original_artifacts_folder is None:
            os.environ.pop("ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID", None)
        else:
            os.environ["ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID"] = self.original_artifacts_folder

    def test_assemble_job_accepted_response_contract(self):
        payload = {"content_id": "YT-20260427-02-r1", "audio_url": "https://example.test/audio.mp3", "script_sections": [{"section": "HOOK", "text": "Smoke section"}]}
        with patch.object(assembly_jobs, "_process_assemble_job", lambda *args, **kwargs: None):
            response = self.client.post("/assemble-job", json=payload, headers=self.headers)
        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertEqual(set(data.keys()), {"job_key", "status"})
        self.assertEqual(data["status"], "processing")
        self.assertTrue(data["job_key"].startswith("assemble_YT-20260427-02-r1_"))

    def test_failed_status_includes_returncode_and_last_80_stderr_lines(self):
        job_key = "assemble_smoke_failed"
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_FAILED, job_key, {"job_key": job_key, "status": "failed", "output_video_url": None, "error_class": "ASSEMBLY", "error_message": "boom", "ffmpeg_returncode": 1, "updated_at": "2026-05-02T00:00:00Z"})
        assembly_jobs.ASSEMBLE_STDERR.mkdir(parents=True, exist_ok=True)
        assembly_jobs._stderr_path(job_key).write_text("\n".join(f"line {index}" for index in range(1, 101)), encoding="utf-8")
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
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_DONE, job_key, {"job_key": job_key, "status": "done", "output_video_url": f"/assemble-jobs/{job_key}/video", "error_class": None, "error_message": None, "updated_at": "2026-05-02T00:00:00Z"})
        assembly_jobs.ASSEMBLE_STDERR.mkdir(parents=True, exist_ok=True)
        assembly_jobs._stderr_path(job_key).write_text("successful ffmpeg stderr\n", encoding="utf-8")
        response = self.client.get(f"/assemble-jobs/{job_key}", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "done")
        self.assertIsNone(data["ffmpeg_stderr_tail"])

    def test_run_failure_preserves_returncode_and_per_job_stderr(self):
        job_key = "assemble_smoke_run_failure"
        command = [sys.executable, "-c", "import sys; sys.stderr.write('diagnostic stderr line one\\nroot cause line two\\n'); sys.exit(7)"]
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
        text = "HOOK: \"Short title, isn't it?\" вЂ” 100% real"
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
                assembly_jobs._prepare_video_segment(Path("/tmp/input.mp4"), Path("/tmp/output.mp4"), 1.0, textfile_path, job_key="assemble_textfile_smoke")
            vf = captured["cmd"][captured["cmd"].index("-vf") + 1]
            self.assertIn("drawtext=", vf)
            self.assertIn("textfile=", vf)
            self.assertNotIn(":text=", vf)
            self.assertNotIn(text, vf)
            self.assertEqual(textfile_path.read_text(encoding="utf-8"), text)
            self.assertEqual(captured["job_key"], "assemble_textfile_smoke")
            self.assertEqual(captured["event"], "prepare_video_segment")

    def test_section_overlay_uses_title_not_long_text(self):
        long_text = "This is the long narration text that should be spoken by audio only and must not appear on screen."
        section = assembly_jobs.ScriptSection(section="HOOK", title="Energy Drain", label="Ignored Label", text=long_text)
        with tempfile.TemporaryDirectory() as tmp:
            textfile_path = assembly_jobs._write_drawtext_textfile(Path(tmp), 0, assembly_jobs._section_text(section))
            overlay = textfile_path.read_text(encoding="utf-8")
        self.assertEqual(overlay, "Energy Drain")
        self.assertNotIn(long_text, overlay)

    def test_section_overlay_uses_label_before_section(self):
        section = assembly_jobs.ScriptSection(section="HOOK", label="Main Problem", text="Long narration that should not be shown")
        self.assertEqual(assembly_jobs._section_text(section), "Main Problem")

    def test_section_overlay_is_capped_to_safe_short_length(self):
        section = assembly_jobs.ScriptSection(title="A" * 80, text="Narration should not be used")
        with tempfile.TemporaryDirectory() as tmp:
            textfile_path = assembly_jobs._write_drawtext_textfile(Path(tmp), 0, assembly_jobs._section_text(section))
            overlay = textfile_path.read_text(encoding="utf-8")
        self.assertEqual(len(overlay), assembly_jobs.ASSEMBLE_OVERLAY_TEXT_LIMIT)
        self.assertEqual(overlay, "A" * assembly_jobs.ASSEMBLE_OVERLAY_TEXT_LIMIT)

    def test_prepare_video_segment_uses_720p_thread_limited_textfile_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp)
            textfile_path = assembly_jobs._write_drawtext_textfile(work_dir, 0, "HOOK")
            captured = {}

            def fake_run(cmd, *, job_key=None, event=None):
                captured["cmd"] = cmd
                captured["job_key"] = job_key
                captured["event"] = event
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with patch.object(assembly_jobs, "_run", fake_run):
                assembly_jobs._prepare_video_segment(Path("/tmp/input.mp4"), work_dir / "output.mp4", 1.0, textfile_path, job_key="assemble_memory_smoke")
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
        response.raise_for_status.side_effect = requests.HTTPError("404 Client Error: Not Found for url: https://pixabay.com/api/music/?key=pixabay-secret-key&q=cinematic+ambient")
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
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_PROCESSING, job_key, {"job_key": job_key, "content_id": "YT-20260427-02-r1", "status": "processing", "output_video_url": None, "output_path": None, "error_class": None, "error_message": None, "ffmpeg_returncode": None, "ffmpeg_stderr_path": None, "ffmpeg_stderr_tail": None, "received_at": "2026-05-02T00:00:00Z", "updated_at": "2026-05-02T00:00:00Z"})
        body = assembly_jobs.AssembleJobRequest(content_id="YT-20260427-02-r1", audio_url="https://example.test/audio.mp3", script_sections=[assembly_jobs.ScriptSection(section="HOOK", title="Hook", text="Long narration should not be used as overlay")])
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
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"final")

        with patch.object(assembly_jobs, "_download_url", fake_download), patch.object(assembly_jobs, "_probe_duration", return_value=5.0), patch.object(assembly_jobs, "_pexels_video_url", return_value="https://example.test/clip.mp4"), patch.object(assembly_jobs, "_prepare_video_segment", fake_prepare), patch.object(assembly_jobs, "_concat_segments", fake_concat), patch.object(assembly_jobs, "_pixabay_music_url", return_value=None), patch.object(assembly_jobs, "_mix_audio", fake_mix), patch.object(assembly_jobs, "_log_event", lambda event, job_key, **fields: events.append((event, fields))):
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
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_PROCESSING, job_key, {"job_key": job_key, "content_id": "YT-20260427-02-r1", "status": "processing", "output_video_url": None, "output_path": None, "error_class": None, "error_message": None, "ffmpeg_returncode": None, "ffmpeg_stderr_path": None, "ffmpeg_stderr_tail": None, "received_at": "2026-05-02T00:00:00Z", "updated_at": "2026-05-02T00:00:00Z"})
        body = assembly_jobs.AssembleJobRequest(content_id="YT-20260427-02-r1", audio_url="https://example.test/audio.mp3", script_sections=[assembly_jobs.ScriptSection(section="HOOK", text="Smoke section")])
        with patch.object(assembly_jobs, "_download_url", side_effect=requests.HTTPError("403 Client Error: Forbidden for url: https://example.test/audio.mp3?key=secret-token")):
            assembly_jobs._process_assemble_job(job_key, body)
        failed_job = assembly_jobs._load_job(assembly_jobs.ASSEMBLE_FAILED, job_key)
        self.assertIsNotNone(failed_job)
        self.assertEqual(failed_job["status"], "failed")
        self.assertNotIn("secret-token", failed_job["error_message"])
        self.assertIn("key=<redacted>", failed_job["error_message"])

    def test_completed_job_with_valid_output_path_returns_video_bytes(self):
        job_key = "assemble_video_ok"
        output_path = assembly_jobs.ASSEMBLE_OUTPUTS / f"{job_key}.mp4"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"video-bytes")
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_DONE, job_key, {"job_key": job_key, "status": "done", "output_video_url": f"/assemble-jobs/{job_key}/video", "output_path": str(output_path), "error_class": None, "error_message": None, "updated_at": "2026-05-02T00:00:00Z"})
        events = []
        with patch.object(assembly_jobs, "_log_event", lambda event, job_key, **fields: events.append((event, fields))):
            response = self.client.get(f"/assemble-jobs/{job_key}/video", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"video-bytes")
        self.assertIn("VIDEO_DOWNLOAD_START", [event for event, _ in events])
        self.assertIn("VIDEO_DOWNLOAD_OK", [event for event, _ in events])
        ok = [fields for event, fields in events if event == "VIDEO_DOWNLOAD_OK"][0]
        self.assertEqual(ok["bytes"], len(b"video-bytes"))

    def test_completed_job_with_missing_output_path_returns_404_and_safe_log(self):
        job_key = "assemble_video_missing"
        missing_path = assembly_jobs.ASSEMBLE_OUTPUTS / "missing.mp4"
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_DONE, job_key, {"job_key": job_key, "status": "done", "output_video_url": f"/assemble-jobs/{job_key}/video", "output_path": str(missing_path), "error_class": None, "error_message": None, "updated_at": "2026-05-02T00:00:00Z"})
        events = []
        with patch.object(assembly_jobs, "_log_event", lambda event, job_key, **fields: events.append((event, fields))):
            response = self.client.get(f"/assemble-jobs/{job_key}/video", headers=self.headers)
        self.assertEqual(response.status_code, 404)
        names = [event for event, _ in events]
        self.assertIn("VIDEO_DOWNLOAD_START", names)
        self.assertIn("VIDEO_DOWNLOAD_NOT_FOUND", names)
        not_found = [fields for event, fields in events if event == "VIDEO_DOWNLOAD_NOT_FOUND"][0]
        self.assertIn("missing.mp4", not_found["output_path"])
        self.assertNotIn("key=", not_found["output_path"])

    def test_state_complete_written_after_output_file_exists(self):
        job_key = "assemble_state_after_output"
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_PROCESSING, job_key, {"job_key": job_key, "content_id": "YT-20260427-02-r1", "status": "processing", "output_video_url": None, "output_path": None, "error_class": None, "error_message": None, "ffmpeg_returncode": None, "ffmpeg_stderr_path": None, "ffmpeg_stderr_tail": None, "received_at": "2026-05-02T00:00:00Z", "updated_at": "2026-05-02T00:00:00Z"})
        body = assembly_jobs.AssembleJobRequest(content_id="YT-20260427-02-r1", audio_url="https://example.test/audio.mp3", script_sections=[assembly_jobs.ScriptSection(section="HOOK", title="Hook", text="Smoke section")])
        events = []

        def fake_download(_url, dest, **_kwargs):
            dest.write_bytes(b"media")

        def fake_prepare(_input_path, output_path, _duration, _textfile_path, *, job_key):
            output_path.write_bytes(b"segment")

        def fake_concat(_segment_paths, output_path, _work_dir, *, job_key):
            output_path.write_bytes(b"concat")

        def fake_mix(_video_path, _voice_path, _music_path, output_path, _duration, *, job_key):
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"final")

        def capture_event(event, job_key, **fields):
            if event == "STATE_COMPLETE_WRITTEN":
                self.assertTrue(Path(fields["output_path"]).exists())
            events.append(event)

        with patch.object(assembly_jobs, "_download_url", fake_download), patch.object(assembly_jobs, "_probe_duration", return_value=5.0), patch.object(assembly_jobs, "_pexels_video_url", return_value="https://example.test/clip.mp4"), patch.object(assembly_jobs, "_prepare_video_segment", fake_prepare), patch.object(assembly_jobs, "_concat_segments", fake_concat), patch.object(assembly_jobs, "_pixabay_music_url", return_value=None), patch.object(assembly_jobs, "_mix_audio", fake_mix), patch.object(assembly_jobs, "_log_event", capture_event):
            assembly_jobs._process_assemble_job(job_key, body)
        self.assertIn("STATE_COMPLETE_WRITTEN", events)
        self.assertTrue(assembly_jobs._output_path(job_key).exists())

    def test_state_complete_not_written_when_output_file_missing(self):
        job_key = "assemble_missing_after_mux"
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_PROCESSING, job_key, {"job_key": job_key, "content_id": "YT-20260427-02-r1", "status": "processing", "output_video_url": None, "output_path": None, "error_class": None, "error_message": None, "ffmpeg_returncode": None, "ffmpeg_stderr_path": None, "ffmpeg_stderr_tail": None, "received_at": "2026-05-02T00:00:00Z", "updated_at": "2026-05-02T00:00:00Z"})
        body = assembly_jobs.AssembleJobRequest(content_id="YT-20260427-02-r1", audio_url="https://example.test/audio.mp3", script_sections=[assembly_jobs.ScriptSection(section="HOOK", title="Hook", text="Smoke section")])
        events = []

        def fake_download(_url, dest, **_kwargs):
            dest.write_bytes(b"media")

        def fake_prepare(_input_path, output_path, _duration, _textfile_path, *, job_key):
            output_path.write_bytes(b"segment")

        def fake_concat(_segment_paths, output_path, _work_dir, *, job_key):
            output_path.write_bytes(b"concat")

        def fake_mix(_video_path, _voice_path, _music_path, _output_path, _duration, *, job_key):
            pass

        with patch.object(assembly_jobs, "_download_url", fake_download), patch.object(assembly_jobs, "_probe_duration", return_value=5.0), patch.object(assembly_jobs, "_pexels_video_url", return_value="https://example.test/clip.mp4"), patch.object(assembly_jobs, "_prepare_video_segment", fake_prepare), patch.object(assembly_jobs, "_concat_segments", fake_concat), patch.object(assembly_jobs, "_pixabay_music_url", return_value=None), patch.object(assembly_jobs, "_mix_audio", fake_mix), patch.object(assembly_jobs, "_log_event", lambda event, job_key, **fields: events.append(event)):
            assembly_jobs._process_assemble_job(job_key, body)
        self.assertNotIn("STATE_COMPLETE_WRITTEN", events)
        self.assertIsNone(assembly_jobs._load_job(assembly_jobs.ASSEMBLE_DONE, job_key))
        self.assertIsNotNone(assembly_jobs._load_job(assembly_jobs.ASSEMBLE_FAILED, job_key))

    def test_status_includes_output_exists_true_when_file_exists(self):
        job_key = "assemble_output_exists"
        output_path = assembly_jobs.ASSEMBLE_OUTPUTS / f"{job_key}.mp4"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"video")
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_DONE, job_key, {"job_key": job_key, "status": "done", "output_video_url": f"/assemble-jobs/{job_key}/video", "output_path": str(output_path), "error_class": None, "error_message": None, "updated_at": "2026-05-02T00:00:00Z"})
        response = self.client.get(f"/assemble-jobs/{job_key}", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["output_video_url"], f"/assemble-jobs/{job_key}/video")
        self.assertTrue(data["output_exists"])

    # в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
    # Restart-safe persistence patch tests
    # в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

    def test_state_store_disabled_when_env_unset_is_noop(self):
        from utils import state_store
        # Env var was scrubbed in setUp; module reads at call time.
        self.assertFalse(state_store.is_enabled())
        self.assertEqual(state_store.write_state("job_x", {"lifecycle_state": "QUEUED"}), "")
        self.assertIsNone(state_store.read_state("job_x"))
        self.assertEqual(state_store.list_all_states(), [])
        self.assertEqual(state_store.upload_artifact("job_x", Path("/tmp/missing.mp4")), "")

    def test_persist_lifecycle_writes_local_emits_log_and_attempts_mirror(self):
        job_key = "assemble_lifecycle_local"
        job = {
            "job_key": job_key,
            "content_id": "YT-LIFECYCLE-01",
            "status": "processing",
        }
        events = []
        mirror_calls = []

        def fake_log(event, job_key, **fields):
            events.append((event, fields))

        def fake_write_state(jk, payload):
            mirror_calls.append((jk, payload.get("lifecycle_state")))
            return ""  # disabled-mirror path

        with patch.object(assembly_jobs, "_log_event", fake_log), \
             patch.object(assembly_jobs.state_store, "write_state", fake_write_state):
            assembly_jobs._persist_lifecycle(
                job_key, assembly_jobs.STATE_STARTED,
                assembly_jobs.ASSEMBLE_PROCESSING, job,
                current_step="started",
            )

        # Local was written.
        local = assembly_jobs._load_job(assembly_jobs.ASSEMBLE_PROCESSING, job_key)
        self.assertIsNotNone(local)
        self.assertEqual(local["lifecycle_state"], assembly_jobs.STATE_STARTED)
        self.assertEqual(local["current_step"], "started")
        self.assertIn("updated_at", local)

        # Lifecycle log fired with the right event name.
        names = [e for e, _ in events]
        self.assertIn(f"LIFECYCLE_{assembly_jobs.STATE_STARTED}", names)

        # Mirror was attempted exactly once with the new lifecycle.
        self.assertEqual(mirror_calls, [(job_key, assembly_jobs.STATE_STARTED)])

    def test_startup_scan_marks_non_terminal_as_interrupted(self):
        # Simulate two persisted states: one non-terminal, one terminal.
        non_terminal = {
            "job_key": "assemble_persisted_running",
            "content_id": "YT-PERSIST-RUN",
            "status": "processing",
            "lifecycle_state": assembly_jobs.STATE_SEGMENT_PREP_START,
            "updated_at": "2026-05-02T00:00:00Z",
        }
        terminal = {
            "job_key": "assemble_persisted_done",
            "content_id": "YT-PERSIST-DONE",
            "status": "done",
            "lifecycle_state": assembly_jobs.STATE_COMPLETE_WRITTEN,
            "updated_at": "2026-05-02T00:00:00Z",
        }

        write_calls = []

        def fake_is_enabled():
            return True

        def fake_list_all_states():
            return [non_terminal, terminal]

        def fake_write_state(jk, payload):
            write_calls.append((jk, payload.get("lifecycle_state")))
            return "drive-id-fake"

        with patch.object(assembly_jobs.state_store, "is_enabled", fake_is_enabled), \
             patch.object(assembly_jobs.state_store, "list_all_states", fake_list_all_states), \
             patch.object(assembly_jobs.state_store, "write_state", fake_write_state):
            summary = assembly_jobs.startup_scan_recover_interrupted()

        self.assertEqual(summary["scanned"], 2)
        self.assertEqual(summary["interrupted"], 1)
        self.assertEqual(summary["terminal"], 1)

        # Non-terminal was rewritten as FAILED_RESTART_INTERRUPTED, terminal untouched.
        self.assertEqual(
            write_calls,
            [("assemble_persisted_running", assembly_jobs.STATE_FAILED_RESTART_INTERRUPTED)],
        )

        # Local FAILED dir got the interrupted record.
        failed_local = assembly_jobs._load_job(
            assembly_jobs.ASSEMBLE_FAILED, "assemble_persisted_running"
        )
        self.assertIsNotNone(failed_local)
        self.assertEqual(failed_local["status"], "failed")
        self.assertEqual(failed_local["error_class"], "RESTART_INTERRUPTED")
        self.assertEqual(
            failed_local["lifecycle_state"],
            assembly_jobs.STATE_FAILED_RESTART_INTERRUPTED,
        )

    def test_list_active_persisted_filters_terminal(self):
        states = [
            {"job_key": "a1", "lifecycle_state": assembly_jobs.STATE_SEGMENT_PREP_START},
            {"job_key": "a2", "lifecycle_state": assembly_jobs.STATE_COMPLETE_WRITTEN},
            {"job_key": "a3", "lifecycle_state": assembly_jobs.STATE_FAILED},
            {"job_key": "a4", "lifecycle_state": assembly_jobs.STATE_FINAL_MUX_START},
        ]
        with patch.object(assembly_jobs.state_store, "is_enabled", lambda: True), \
             patch.object(assembly_jobs.state_store, "list_all_states", lambda: states):
            active = assembly_jobs.list_active_persisted()
        keys = sorted(s["job_key"] for s in active)
        self.assertEqual(keys, ["a1", "a4"])

    # в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
    # Blocker fixes
    # в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

    def test_state_store_requires_both_env_vars_for_enabled(self):
        from utils import state_store
        # Only state folder set в†’ still disabled.
        os.environ["ASSEMBLY_STATE_DRIVE_FOLDER_ID"] = "state-folder-xyz"
        try:
            self.assertFalse(state_store.is_enabled())
        finally:
            del os.environ["ASSEMBLY_STATE_DRIVE_FOLDER_ID"]

        # Only artifacts folder set в†’ still disabled.
        os.environ["ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID"] = "artifacts-folder-xyz"
        try:
            self.assertFalse(state_store.is_enabled())
        finally:
            del os.environ["ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID"]

        # Both set в†’ enabled.
        os.environ["ASSEMBLY_STATE_DRIVE_FOLDER_ID"] = "state-folder-xyz"
        os.environ["ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID"] = "artifacts-folder-xyz"
        try:
            self.assertTrue(state_store.is_enabled())
        finally:
            del os.environ["ASSEMBLY_STATE_DRIVE_FOLDER_ID"]
            del os.environ["ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID"]


    def test_state_store_partial_config_public_functions_are_noop(self):
        from utils import state_store
        os.environ["ASSEMBLY_STATE_DRIVE_FOLDER_ID"] = "state-folder-xyz"
        try:
            with patch.object(state_store.drive_upload, "upsert_json_state", side_effect=AssertionError("must not be called")), \
                 patch.object(state_store.drive_upload, "read_json_state", side_effect=AssertionError("must not be called")), \
                 patch.object(state_store.drive_upload, "list_json_state_files", side_effect=AssertionError("must not be called")), \
                 patch.object(state_store.drive_upload, "upload_file_to_folder", side_effect=AssertionError("must not be called")):
                self.assertEqual(state_store.write_state("job_x", {"lifecycle_state": "QUEUED"}), "")
                self.assertIsNone(state_store.read_state("job_x"))
                self.assertEqual(state_store.list_all_states(), [])
                self.assertEqual(state_store.upload_artifact("job_x", Path("/tmp/missing.mp4")), "")
        finally:
            del os.environ["ASSEMBLY_STATE_DRIVE_FOLDER_ID"]

    def test_artifact_upload_failure_prevents_complete_when_persistence_enabled(self):
        job_key = "assemble_artifact_mirror_failure"
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_PROCESSING, job_key, {"job_key": job_key, "content_id": "YT-20260427-02-r1", "status": "processing", "output_video_url": None, "output_path": None, "error_class": None, "error_message": None, "ffmpeg_returncode": None, "ffmpeg_stderr_path": None, "ffmpeg_stderr_tail": None, "received_at": "2026-05-02T00:00:00Z", "updated_at": "2026-05-02T00:00:00Z"})
        body = assembly_jobs.AssembleJobRequest(content_id="YT-20260427-02-r1", audio_url="https://example.test/audio.mp3", script_sections=[assembly_jobs.ScriptSection(section="HOOK", title="Hook", text="Smoke section")])
        events = []

        def fake_download(_url, dest, **_kwargs):
            dest.write_bytes(b"media")

        def fake_prepare(_input_path, output_path, _duration, _textfile_path, *, job_key):
            output_path.write_bytes(b"segment")

        def fake_concat(_segment_paths, output_path, _work_dir, *, job_key):
            output_path.write_bytes(b"concat")

        def fake_mix(_video_path, _voice_path, _music_path, output_path, _duration, *, job_key):
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"final")

        with patch.object(assembly_jobs, "_download_url", fake_download), \
             patch.object(assembly_jobs, "_probe_duration", return_value=5.0), \
             patch.object(assembly_jobs, "_pexels_video_url", return_value="https://example.test/clip.mp4"), \
             patch.object(assembly_jobs, "_prepare_video_segment", fake_prepare), \
             patch.object(assembly_jobs, "_concat_segments", fake_concat), \
             patch.object(assembly_jobs, "_pixabay_music_url", return_value=None), \
             patch.object(assembly_jobs, "_mix_audio", fake_mix), \
             patch.object(assembly_jobs.state_store, "is_enabled", lambda: True), \
             patch.object(assembly_jobs.state_store, "upload_artifact", return_value=""), \
             patch.object(assembly_jobs, "_log_event", lambda event, job_key, **fields: events.append(event)):
            assembly_jobs._process_assemble_job(job_key, body)

        self.assertNotIn("STATE_COMPLETE_WRITTEN", events)
        self.assertIsNone(assembly_jobs._load_job(assembly_jobs.ASSEMBLE_DONE, job_key))
        failed_job = assembly_jobs._load_job(assembly_jobs.ASSEMBLE_FAILED, job_key)
        self.assertIsNotNone(failed_job)
        self.assertEqual(failed_job["status"], "failed")
        self.assertEqual(failed_job["error_class"], "ARTIFACT_MIRROR")

    def test_video_endpoint_returns_409_when_job_known_but_not_complete(self):
        job_key = "assemble_video_not_ready"
        # Save a job in PROCESSING with a non-terminal lifecycle.
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_PROCESSING, job_key, {
            "job_key": job_key,
            "status": "processing",
            "lifecycle_state": assembly_jobs.STATE_SEGMENT_PREP_START,
            "current_step": "segment_prep",
            "error_class": None,
            "error_message": None,
            "updated_at": "2026-05-02T00:00:00Z",
        })
        response = self.client.get(f"/assemble-jobs/{job_key}/video", headers=self.headers)
        self.assertEqual(response.status_code, 409)
        detail = response.json()["detail"]
        self.assertEqual(detail["error_class"], "NOT_READY")
        self.assertEqual(detail["job_key"], job_key)
        self.assertEqual(detail["lifecycle_state"], assembly_jobs.STATE_SEGMENT_PREP_START)
        self.assertEqual(detail["status"], "processing")

    def test_video_endpoint_returns_410_when_complete_but_no_artifact_and_persistence_on(self):
        job_key = "assemble_video_artifact_lost"
        missing_path = assembly_jobs.ASSEMBLE_OUTPUTS / f"{job_key}.mp4"
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_DONE, job_key, {
            "job_key": job_key,
            "status": "done",
            "lifecycle_state": assembly_jobs.STATE_COMPLETE_WRITTEN,
            "output_video_url": f"/assemble-jobs/{job_key}/video",
            "output_path": str(missing_path),  # does not exist
            "drive_file_id": None,
            "error_class": None,
            "error_message": None,
            "updated_at": "2026-05-02T00:00:00Z",
        })
        # Force persistence-enabled view.
        with patch.object(assembly_jobs.state_store, "is_enabled", lambda: True):
            response = self.client.get(f"/assemble-jobs/{job_key}/video", headers=self.headers)
        self.assertEqual(response.status_code, 410)
        detail = response.json()["detail"]
        self.assertEqual(detail["error_class"], "ARTIFACT_LOST")
        self.assertEqual(detail["job_key"], job_key)

    def test_video_endpoint_returns_200_with_drive_file_id_when_local_missing(self):
        job_key = "assemble_video_drive_fallback"
        missing_path = assembly_jobs.ASSEMBLE_OUTPUTS / f"{job_key}.mp4"
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_DONE, job_key, {
            "job_key": job_key,
            "status": "done",
            "lifecycle_state": assembly_jobs.STATE_COMPLETE_WRITTEN,
            "output_video_url": f"/assemble-jobs/{job_key}/video",
            "output_path": str(missing_path),
            "drive_file_id": "drive-id-abc-123",
            "error_class": None,
            "error_message": None,
            "updated_at": "2026-05-02T00:00:00Z",
        })
        response = self.client.get(f"/assemble-jobs/{job_key}/video", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["drive_file_id"], "drive-id-abc-123")
        self.assertEqual(body["job_key"], job_key)
        self.assertEqual(body["lifecycle_state"], assembly_jobs.STATE_COMPLETE_WRITTEN)

    def test_assemble_jobs_health_route_is_removed(self):
        # Unauthenticated access must not leak job keys via this route.
        response = self.client.get("/assemble-jobs-health")
        self.assertEqual(response.status_code, 404)
        # Authenticated still 404 вЂ” route does not exist anymore.
        response = self.client.get("/assemble-jobs-health", headers=self.headers)
        self.assertEqual(response.status_code, 404)

    def test_health_does_not_expose_assembly_job_keys(self):
        import app as runner_app
        runner_app._reset_assembly_health_summary_for_tests()
        self.addCleanup(runner_app._reset_assembly_health_summary_for_tests)
        runner_app._set_assembly_health_summary(
            persisted_non_terminal_count=1,
            known_unreconciled_count=1,
            reconciled=False,
        )
        with patch.object(runner_app, "ASSEMBLE_PROCESSING", self.root / "no-processing"):
            body = runner_app.health()
        self.assertIn("assembly", body)
        self.assertEqual(body["assembly"]["persisted_non_terminal_count"], 1)
        self.assertNotIn("persisted_non_terminal_keys", body["assembly"])
        self.assertNotIn("local_processing_keys", body["assembly"])
        self.assertFalse(body["ok"])



    def test_terminal_state_mirror_failure_prevents_complete_when_artifact_uploaded(self):
        job_key = "assemble_terminal_state_mirror_failure"
        assembly_jobs._save_job(assembly_jobs.ASSEMBLE_PROCESSING, job_key, {"job_key": job_key, "content_id": "YT-20260427-02-r1", "status": "processing", "output_video_url": None, "output_path": None, "error_class": None, "error_message": None, "ffmpeg_returncode": None, "ffmpeg_stderr_path": None, "ffmpeg_stderr_tail": None, "received_at": "2026-05-02T00:00:00Z", "updated_at": "2026-05-02T00:00:00Z"})
        body = assembly_jobs.AssembleJobRequest(content_id="YT-20260427-02-r1", audio_url="https://example.test/audio.mp3", script_sections=[assembly_jobs.ScriptSection(section="HOOK", title="Hook", text="Smoke section")])
        events = []

        def fake_download(_url, dest, **_kwargs):
            dest.write_bytes(b"media")

        def fake_prepare(_input_path, output_path, _duration, _textfile_path, *, job_key):
            output_path.write_bytes(b"segment")

        def fake_concat(_segment_paths, output_path, _work_dir, *, job_key):
            output_path.write_bytes(b"concat")

        def fake_mix(_video_path, _voice_path, _music_path, output_path, _duration, *, job_key):
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"final")

        def fake_write_state(_job_key, payload):
            if payload.get("lifecycle_state") == assembly_jobs.STATE_COMPLETE_WRITTEN:
                return ""
            return "state-id-ok"

        with patch.object(assembly_jobs, "_download_url", fake_download), \
             patch.object(assembly_jobs, "_probe_duration", return_value=5.0), \
             patch.object(assembly_jobs, "_pexels_video_url", return_value="https://example.test/clip.mp4"), \
             patch.object(assembly_jobs, "_prepare_video_segment", fake_prepare), \
             patch.object(assembly_jobs, "_concat_segments", fake_concat), \
             patch.object(assembly_jobs, "_pixabay_music_url", return_value=None), \
             patch.object(assembly_jobs, "_mix_audio", fake_mix), \
             patch.object(assembly_jobs.state_store, "is_enabled", lambda: True), \
             patch.object(assembly_jobs.state_store, "upload_artifact", return_value="drive-artifact-id"), \
             patch.object(assembly_jobs.state_store, "write_state", fake_write_state), \
             patch.object(assembly_jobs, "_log_event", lambda event, job_key, **fields: events.append(event)):
            assembly_jobs._process_assemble_job(job_key, body)

        self.assertNotIn("STATE_COMPLETE_WRITTEN", events)
        self.assertIsNone(assembly_jobs._load_job(assembly_jobs.ASSEMBLE_DONE, job_key))
        failed_job = assembly_jobs._load_job(assembly_jobs.ASSEMBLE_FAILED, job_key)
        self.assertIsNotNone(failed_job)
        self.assertEqual(failed_job["status"], "failed")
        self.assertEqual(failed_job["error_class"], "STATE_MIRROR")


class AssemblyHealthCacheTests(unittest.TestCase):
    """Tests for the cached /health summary and background startup scan.
    See PR #25 third fix: separate known_interrupted_count from
    known_unreconciled_count; /health must remain cache-only."""

    def setUp(self):
        import app as app_module
        self.app_module = app_module
        # Ensure each test starts from default cached state.
        app_module._reset_assembly_health_summary_for_tests()
        self.addCleanup(app_module._reset_assembly_health_summary_for_tests)
        self.client = TestClient(app_module.app)

    def _set_summary(self, **updates):
        self.app_module._set_assembly_health_summary(**updates)

    # ── A) /health does not scan Drive on request ────────────────────
    def test_health_endpoint_does_not_scan_drive(self):
        import assembly_jobs

        def boom(*args, **kwargs):
            raise AssertionError("Drive scan must not be called from /health")

        with patch.object(assembly_jobs, "list_active_persisted", boom), \
             patch.object(assembly_jobs.state_store, "list_all_states", boom):
            response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("assembly", body)

    # ── B) startup schedules background scan and does not await it ──
    def test_startup_schedules_background_scan_without_awaiting(self):
        import asyncio as _asyncio

        scheduled = []

        def fake_schedule():
            scheduled.append(True)
            return None  # not a real task; we are not awaiting it

        # Patch the scheduling indirection so the startup hook calls our fake.
        # If the hook tried to await Drive reconciliation, this test would
        # block (or call the real scan); since we replace _schedule_*, the
        # hook returns immediately after calling fake_schedule().
        with patch.object(self.app_module, "_schedule_assembly_startup_scan", fake_schedule):
            _asyncio.run(self.app_module.run_assembly_startup_scan())

        self.assertEqual(scheduled, [True])

    # ── C) cached known_unreconciled_count makes /health ok=false ───
    def test_cached_known_unreconciled_count_makes_health_not_ok(self):
        self._set_summary(
            scan_state="complete",
            known_unreconciled_count=1,
            persisted_non_terminal_count=0,
            known_interrupted_count=0,
            reconciled=False,
        )
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual(body["assembly"]["known_unreconciled_count"], 1)

    # ── D) successful scan with interrupted jobs: ok=true, count recorded ─
    def test_successful_interrupted_recovery_records_count_and_health_ok(self):
        import asyncio as _asyncio

        fake_summary = {"scanned": 2, "interrupted": 1, "terminal": 1}

        with patch.object(self.app_module, "startup_scan_recover_interrupted",
                          return_value=fake_summary), \
             patch.object(self.app_module, "_assembly_startup_scan_timeout_seconds",
                          return_value=30):
            _asyncio.run(self.app_module._run_assembly_startup_scan_background())

        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        assembly = body["assembly"]
        self.assertTrue(body["ok"])
        self.assertEqual(assembly["scan_state"], "complete")
        self.assertEqual(assembly["known_interrupted_count"], 1)
        self.assertEqual(assembly["persisted_non_terminal_count"], 0)
        self.assertEqual(assembly["known_unreconciled_count"], 0)
        self.assertTrue(assembly["reconciled"])


if __name__ == "__main__":
    unittest.main()

