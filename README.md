# yt-video-render-runner

## Environment variables

Required for runner authentication:

- `RUNNER_AUTH_TOKEN`

Required for cinematic assembly jobs:

- `PEXELS_API_KEY` — used by `POST /assemble-job` to search and download cinematic video clips from Pexels.
- `PIXABAY_API_KEY` — used by `POST /assemble-job` to search and download royalty-free cinematic ambient music from Pixabay.

Optional runtime controls:

- `MAX_CONCURRENT_RENDERS` — defaults to `3`.
- `STALE_JOB_TIMEOUT_SECONDS` — defaults to `3600`.

External binary requirement:

- `ffmpeg` and `ffprobe` must be available in the runtime environment.
