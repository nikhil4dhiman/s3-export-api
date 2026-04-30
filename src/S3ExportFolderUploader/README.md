# S3ExportFolderUploader

A .NET 8 console CLI that drives the `s3-export-api` end-to-end from a single
folder. It starts an export session, walks the folder, uploads every file
(single-shot or multipart based on size) through Approach A or B, and
optionally finalises the export.

## Features

- **One command**: scan folder → start session → upload all files → complete.
- **Per-file routing**: small files use the single-shot endpoint; files larger
  than `--threshold` are split into S3-compliant ≥ 5 MiB chunks and uploaded
  as multipart.
- **Both approaches**: `--approach a` (streaming zip, sequential parts) and
  `--approach b` (staged zip, parallel parts).
- **Retry with exponential backoff** on transient HTTP failures (`408`, `425`,
  `429`, `5xx`, network/timeout). Configurable via `--max-retries`.
- **Resume across runs**. A JSON state file (default
  `<folder>/.s3-export-state.json`) records the export id, completed files,
  and — for Approach B — in-progress multipart uploads with their per-part
  ETags. Re-run with `--resume` to skip already-uploaded files and pick up
  unfinished multipart uploads where they left off. Removed on successful
  `--complete`.
- **Concurrency control** via `--parallelism N` (Approach B only — Approach A
  is intrinsically sequential because the server holds a per-export write
  lock).
- **No intermediate buffers**: every part is read straight from disk via a
  bounded `FileSliceStream`. No temp files, no in-memory copies.

## Build & run

```bash
dotnet build S3ExportApi.sln

# Approach A — sequential streaming upload of an entire folder
dotnet run --project src/S3ExportFolderUploader -- \
    --approach a --folder ./outbox --complete

# Approach B — recursive, 16 MB parts, 8-way parallelism, resumable
dotnet run --project src/S3ExportFolderUploader -- \
    --approach b --folder ./outbox --recursive \
    --part-size 16mb --parallelism 8 --resume --complete

# Resume an interrupted run (state file is still in the folder)
dotnet run --project src/S3ExportFolderUploader -- \
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
| `--state-file <p>`  | Resume-state file path (default `<folder>/.s3-export-state.json`).          |
| `--resume`          | Reuse state file: skip completed files, continue in-progress multiparts.    |

## Resume semantics

- **Approach B** uploads each file as an independent S3 object, so a partially-
  uploaded multipart file can be picked up exactly where it stopped: the saved
  `uploadId` is reused and only the missing part numbers are sent.
- **Approach A** keeps server-side state (the in-memory `ZipArchive`) tied to
  the API process. Resume granularity is therefore **per-file**: completed
  files are skipped, but a half-uploaded file restarts from part 1. If the API
  process itself was restarted, the export id is no longer valid and the run
  must start over (start a fresh session without `--resume`).

On successful `--complete` the state file is deleted automatically.

## Exit codes

| Code | Meaning                                                                 |
|------|-------------------------------------------------------------------------|
| `0`  | All uploads (and `--complete`, if requested) succeeded.                 |
| `1`  | An upload failed after exhausting retries. State file is preserved for `--resume`. |
| `2`  | Bad arguments (missing folder, invalid sizes, etc.).                    |
