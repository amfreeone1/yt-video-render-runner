import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
        self.addCleanup(self._restore_token)

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


if __name__ == "__main__":
    unittest.main()
