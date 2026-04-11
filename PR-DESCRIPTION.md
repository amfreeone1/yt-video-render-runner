# PR: validation-first-download-guard

**Branch:** `fix/download-validation-guard`
**Target:** `main`
**Scope:** Minimal — download validation + CLAUDE.md only

---

## Changes

### 1. `CLAUDE.md` (new file)
Canonical developer guide: architecture, API surface, job contract,
known risks, error taxonomy, PR discipline rules.

### 2. `app.py` — three surgical edits

| # | What | Why |
|---|------|-----|
| A | `confirm=t` added to GDrive direct-download URL | Bypasses virus-scan interstitial for large files |
| B | `validate_downloaded_audio()` function added | Fail-fast guard: size check, HTML magic-byte check, optional ffprobe |
| C | Validation call inserted after `download_file()` | Ensures bad downloads never reach ffmpeg |

### 3. No other changes
- No queue refactor
- No thread pool change
- No API contract redesign
- No `video_url` field removal

---

## Error messages introduced

| Value | Trigger |
|-------|---------|
| `downloaded_audio_missing` | File not on disk after download |
| `downloaded_audio_too_small: N bytes` | File < 1 KB |
| `downloaded_audio_is_html` | First 512 bytes contain `<html` or `<!doctype` |
| `ffprobe_invalid_audio: ...` | ffprobe finds no audio stream (only if ffprobe available) |

All of these flow into the existing `error_message` field on the failed job JSON.
Activepieces polling will see `status: "failed"` + structured `error_message`.

---

## Risk assessment

| Risk | Mitigation |
|------|-----------|
| `confirm=t` still fails on some GDrive files | Validation catches it — job fails with clear message instead of silent corruption |
| ffprobe not installed on Render | Guard is optional (`shutil.which` check) — degrades gracefully |
| Validation adds ~50ms per job | Negligible vs 180s download timeout |

---

## Test plan

1. Submit job with valid small GDrive audio → should complete normally
2. Submit job with invalid/fake `audio_url` → should fail with `downloaded_audio_missing` or `downloaded_audio_too_small`
3. Submit job with URL returning HTML page → should fail with `downloaded_audio_is_html`
4. If ffprobe available: submit job with non-audio file (e.g. PNG) → should fail with `ffprobe_invalid_audio`

---

## Next PR candidates (out of scope here)

- [ ] `ThreadPoolExecutor` with max_workers cap
- [ ] Download retry with exponential backoff
- [ ] `video_url` field deprecation
- [ ] `mark-failed` / `mark-complete` response shape alignment
