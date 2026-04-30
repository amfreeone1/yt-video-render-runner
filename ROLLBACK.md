# Rollback: OAuth2 Drive upload auth

## Rollback Plan

- Previous working version: b242ae5 (PR #16 merge commit)
- Rollback command: `git revert c2fe874`
- Verification step: drive_upload.py falls back to service account auth (will skip upload due to missing SA env vars, non-fatal)
