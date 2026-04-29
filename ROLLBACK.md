# Rollback: auto Drive upload on render complete

## Rollback Plan

- Previous working version: 8b9c0722cb3bf158a49a8a8fa325f3014d2feacf
- Rollback command: `git revert <this PR commit>`
- Verification step: render job completes, artifact endpoint still returns 200, drive_file_id field present in job state (empty string if upload skipped)
