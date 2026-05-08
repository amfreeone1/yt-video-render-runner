# yt-video-render-runner

## Environment variables

Required for runner authentication:

- `RUNNER_AUTH_TOKEN`

Required for cinematic assembly jobs:

- `PEXELS_API_KEY` — used by `POST /assemble-job` to search and download cinematic video clips from Pexels.
- `PIXABAY_API_KEY` — used by `POST /assemble-job` to search and download royalty-free cinematic ambient music from Pixabay.

Required for Drive raw upload MCP:

- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REFRESH_TOKEN`

Optional for Drive raw upload MCP:

- `GOOGLE_DRIVE_FOLDER_ID` — when set, uploaded JPEG files are created inside this Drive folder.

Optional runtime controls:

- `MAX_CONCURRENT_RENDERS` — defaults to `3`.
- `STALE_JOB_TIMEOUT_SECONDS` — defaults to `3600`.

External binary requirement:

- `ffmpeg` and `ffprobe` must be available in the runtime environment.

## Drive raw upload MCP

The service exposes a separate MCP surface at:

```text
https://<render-service>.onrender.com/mcp
```

On the current Render service, the URL shape is:

```text
https://yt-video-render-runner.onrender.com/mcp
```

Tool exposed:

```text
upload_image_to_drive_and_share
```

Behavior:

- Accepts raw `image/jpeg` bytes.
- Uploads the file to Google Drive with Drive `files.create` as `image/jpeg`.
- Uses the request-provided filename, defaulting to `hafiz_ai_concept_instagram.jpg`.
- Does not create Google Docs, Slides, or Sheets.
- Calls Drive `permissions.create` with `type=anyone` and `role=reader`.
- Returns the public Drive `webViewLink`.
- Does not publish to Instagram or any social platform.

Direct raw upload example:

```bash
curl -X POST \
  "https://<render-service>.onrender.com/mcp?filename=hafiz_ai_concept_instagram.jpg" \
  -H "Content-Type: image/jpeg" \
  --data-binary @image.jpg
```

MCP JSON-RPC clients can call `tools/list` and then `tools/call` with `image_base64` and optional `filename`.

### Security notes

- `upload_image_to_drive_and_share` is a Drive write action and should be treated as requiring user confirmation by MCP clients.
- `/mcp` is intentionally separate from existing render job endpoints.
- Existing `/render-jobs` and `/upload-jobs` authentication behavior is unchanged.
- `/mcp` currently has no app-level authentication guard. Until an auth layer is added, deploy it only where the public write risk is acceptable or protect it at the edge/platform layer.
- Do not commit real OAuth secrets. Use Render environment variables.
