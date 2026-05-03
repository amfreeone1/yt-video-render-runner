# ROLLBACK

This file documents how to revert each merged change. Append new entries
to the top so the most recent change is first.

---

## Rollback: feat/assembly-restart-safe-persistence

**Scope:** Restart-safe assembly job persistence. Adds `utils/state_store.py`,
extends `utils/drive_upload.py` with read/list/upsert primitives, wires
lifecycle state mirroring + startup recovery scan into `assembly_jobs.py`,
adds `/health` enrichment in `app.py`.

### To roll back

1. `git revert <merge_commit_sha>` on `main`, OR
2. In the Render dashboard: redeploy the previous successful commit
   (the one immediately before this PR's merge commit).

No database migrations. No changes to `/render-jobs/*` shape. No changes
to upload flow shape.

### Behavioral changes that revert on rollback

- `/assemble-job` will no longer reject submissions when persistence fails;
  it will accept them and silently lose them on the next Render restart.
- `/assemble-jobs/{job_key}` will return 404 for any job whose local
  filesystem state was wiped, even if a Drive mirror exists.
- `/assemble-jobs/{job_key}/video` will return a bare 404 instead of a
  structured 200 with `drive_file_id` when the local artifact is missing
  but the Drive mirror has it.
- `/health` will revert to the original shape: `ok`, `service`,
  `ffmpeg_ready`, `time`, `instance_id`, `uptime_seconds`, `active_jobs`.
  No `assembly` field. `ok` will not reflect persisted-non-terminal jobs.
- No startup scan; non-terminal assembly jobs disappear silently on
  every Render instance recycle.

### Env vars added by this PR (safe to leave set; ignored on rollback)

- `ASSEMBLY_STATE_DRIVE_FOLDER_ID` — Drive folder ID for state JSON files.
- `ASSEMBLY_ARTIFACTS_DRIVE_FOLDER_ID` — Drive folder ID for final MP4
  artifacts. Independent from the existing `GOOGLE_DRIVE_FOLDER_ID` used
  by the `/render-jobs` flow.

### Side-effects to clean up after rollback (optional)

- The two Drive folders accumulate state JSON files and MP4 artifacts.
  These are safe to leave in place. To delete: open each folder in the
  Drive UI and bulk-delete. No code references them after rollback.

### Files touched by this PR

- `utils/drive_upload.py` (extended; original `upload_file_to_drive(file_path)`
  signature preserved for `app.py` callers)
- `utils/state_store.py` (new)
- `assembly_jobs.py` (lifecycle states + persistence wiring)
- `app.py` (startup scan hook + `/health` enrichment)
- `tests/test_assembly_jobs_smoke.py` (existing tests preserved verbatim;
  4 new unittest-compatible tests appended)
- `ROLLBACK.md` (this entry)

### Smoke test command

```
python -m unittest -v tests/test_assembly_jobs_smoke.py
```

All existing tests must continue to pass. New tests cover state-store
disabled noop, lifecycle persistence path, startup scan recovery, and
non-terminal filtering.
