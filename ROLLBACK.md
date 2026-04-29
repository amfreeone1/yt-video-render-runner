# Rollback: upload video source artifact

This PR changes upload jobs so that a provided `render_job_key` resolves the upload video source to the runner artifact endpoint:

```text
https://yt-video-render-runner.onrender.com/render-jobs/{render_job_key}/artifact
```

## Rollback procedure

Revert this PR.

Expected rollback effect:

- Upload jobs return to using the submitted `video` object directly.
- `source="drive"` again requires `video.drive_file_id`.
- The worker no longer rewrites upload video source from `render_job_key` to the render artifact endpoint.
- URL downloads no longer add runner auth specifically for self-hosted artifact fetch unless preserved elsewhere.
