# Rollback: PR #18 env parser and upload job lookup semantics

Council decision reference: ACS-001

## Rollback Plan

- Previous working version: c088e07 (PR #17 merge commit)
- Rollback command: `git revert <PR #18 merge commit>`
- Verification step: runner starts with valid `STALE_JOB_TIMEOUT_SECONDS`; `GET /upload-jobs/{upload_job_key}` returns existing upload job status or structured missing-state error.
