# S3ExportUploader.Py

A Python 3 CLI companion for `s3-export-api`. It walks a configured folder,
starts an export session, uploads every file (single-shot or multipart based
on size) through Approach A or Approach B, and optionally finalises the
export.

**Standard library only** — no third-party packages. Runs on any Python ≥ 3.10.

## Features

- One command: scan folder → start session → upload all files → complete.
- Per-file routing: small files use the single-shot endpoint; large files are
  split into S3-compliant ≥ 5 MiB chunks and uploaded as multipart.
- Works with both **Approach A** (streaming zip, sequential parts) and
  **Approach B** (staged zip, parallel parts).
- **Retry with exponential backoff** on transient HTTP failures (`429`, `5xx`,
  network errors). Retry count is configurable (`--max-retries`).
- **Resume across runs.** A JSON state file (default
  `<folder>/.s3-export-state.json`) records the export id, completed files,
  and — for Approach B — in-progress multipart uploads with their per-part
  ETags. Re-run with `--resume` to skip already-uploaded files and pick up
  unfinished multipart uploads where they left off.
- **Concurrency control** via `--parallelism` (Approach B only; Approach A is
  intrinsically sequential because the server holds a per-export write lock).
- Streams data straight from disk — no intermediate buffers, no temp files.

## Usage

```bash
# Approach A — sequential parts streamed into a single zip
python3 src/S3ExportUploader.Py/s3_export_uploader.py \
    --approach a --folder ./outbox --complete

# Approach B — staged parallel multipart, recursive folder, 16 MB parts
python3 src/S3ExportUploader.Py/s3_export_uploader.py \
    --approach b --folder ./outbox --recursive \
    --part-size 16mb --parallelism 8 --complete

# Resume an interrupted export
python3 src/S3ExportUploader.Py/s3_export_uploader.py \
    --approach b --folder ./outbox --recursive --resume --complete
```

## Options

| Option              | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `--api <url>`       | API base URL (default `http://localhost:5000`).                             |
| `--approach <a\|b>` | `a` = streaming zip, `b` = staged zip (required).                           |
| `--folder <path>`   | Folder whose files will be uploaded (required).                             |
| `--recursive`       | Descend into subfolders. Remote names use POSIX-relative paths.             |
| `--user <id>`       | `userId` for Approach A (default `anonymous`).                              |
| `--export-id <id>`  | Reuse an existing export instead of starting a new one.                     |
| `--complete`        | Call `/complete` after uploads finish.                                      |
| `--part-size <sz>`  | Multipart part size — `8mb`, `16mb`, `1gb`. Min `5mb`. Default `8mb`.       |
| `--threshold <sz>`  | Use multipart for files larger than this (default = `--part-size`).         |
| `--parallelism <n>` | Parallel parts for Approach B (default `4`). Approach A is always 1.        |
| `--max-retries <n>` | Retries per HTTP request on transient failures (default `5`).               |
| `--timeout <s>`     | Per-request HTTP timeout in seconds (default `600`).                        |
| `--state-file <p>`  | Resume-state path (default `<folder>/.s3-export-state.json`).               |
| `--resume`          | Reuse state file: skip completed files, continue in-progress multiparts.    |

## Resume semantics

- **Approach B** uploads each file as an independent S3 object, so a partially-
  uploaded multipart file can be picked up exactly where it stopped: the
  saved `uploadId` is reused and only the missing part numbers are sent.
- **Approach A** keeps server-side state (the in-memory `ZipArchive`) tied to
  the API process. Resume granularity is therefore **per-file**: completed
  files are skipped, but a half-uploaded file restarts from part 1. If the API
  process itself was restarted, the export id is no longer valid and the run
  must start over (start a fresh session without `--resume`).

On successful `--complete` the state file is deleted automatically.

## Exit codes

| Code | Meaning                                              |
|------|------------------------------------------------------|
| `0`  | All uploads (and `--complete`, if requested) succeeded. |
| `1`  | An upload failed after exhausting retries. State file is preserved for `--resume`. |
| `2`  | Bad arguments (missing folder, invalid sizes, etc.). |
