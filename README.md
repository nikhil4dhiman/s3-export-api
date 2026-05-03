# s3-export-api

A .NET 8 Web API that uploads user files to an Amazon S3 bucket **without writing anything to local disk**. Two approaches are implemented side-by-side so you can choose the one that fits your architecture.

---

## How to Run

```bash
dotnet restore
export AWS__Region=us-east-1
export S3__BucketName=your-bucket
dotnet run --project src/S3ExportApi
```

Swagger UI is available at: **http://localhost:5000/swagger**

> **AWS credentials** are resolved via the standard AWS SDK credential chain (environment variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, `~/.aws/credentials`, EC2 instance profile, etc.).

---

## Memory Profile

| Resource | Footprint |
|---|---|
| Active Approach A export | ~5–10 MB (one S3 part buffer per session) |
| Active Approach B file upload | negligible (streamed directly to S3) |
| Temp files on disk | **none** |

---

## Approach A — Streaming ZIP (in-memory, stateful)

Files are written directly into a `ZipArchive` whose underlying stream buffers data into 5 MB chunks and flushes them as S3 multipart upload parts. The ZIP is never materialised on disk; it exists only as in-flight bytes in memory.

**Best for:** single-instance deployments, lowest latency, simplest client.

### Endpoint Summary

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/approach-a/exports/start?userId=` | Start export session, returns `{ exportId }` |
| `POST` | `/api/approach-a/exports/{exportId}/files?fileName=` | Upload small file (stream body) |
| `POST` | `/api/approach-a/exports/{exportId}/files/multipart/start` | Begin large-file upload, returns `{ fileUploadId }` |
| `PUT` | `/api/approach-a/exports/{exportId}/files/multipart/{fileUploadId}/parts` | Append a part to the open entry |
| `POST` | `/api/approach-a/exports/{exportId}/files/multipart/{fileUploadId}/complete` | Close entry, release write lock |
| `POST` | `/api/approach-a/exports/{exportId}/complete` | Finalise ZIP, complete S3 upload, returns `{ exportId, s3Url }` |
| `DELETE` | `/api/approach-a/exports/{exportId}` | Abort and discard the export |

### Flow

```
POST /start          → creates S3 multipart upload + in-memory ZipArchive
POST /files          → appends a zip entry (small file, single request)
POST /files/multipart/start  → opens a zip entry, acquires write lock
PUT  /files/multipart/{id}/parts  → streams bytes into the open entry
POST /files/multipart/{id}/complete  → closes entry, releases write lock
POST /complete       → flushes ZIP central directory → CompleteMultipartUpload on S3
```

---

## Approach B — Staged ZIP (stateless per request)

Each file is uploaded directly to `exports/{exportId}/staging/{fileName}` in S3 as an independent object (with `ChecksumAlgorithm.CRC32` so that S3 stores a native checksum). When `/complete` is called the server **synchronously** builds the final ZIP using S3's `CopyPartAsync` (server-side copy) for file data ≥ 5 MiB — these bytes never transit the API host. Small files and ZIP metadata (local file headers, central directory) are uploaded as regular parts. The result is `exports/{exportId}/final.zip`. Staging cleanup is opt-in (`S3:DeleteStagingOnComplete`) and runs as fire-and-forget — the recommended path is an S3 lifecycle rule on the `staging/` prefix.

**Best for:** horizontally scaled, stateless API deployments.

### Endpoint Summary

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/approach-b/exports/start` | Allocate an exportId (stateless), returns `{ exportId }` |
| `POST` | `/api/approach-b/exports/{exportId}/files?fileName=` | Stream file to S3 staging prefix |
| `POST` | `/api/approach-b/exports/{exportId}/files/multipart/start` | Start S3 multipart upload to staging, returns `{ fileUploadId, key }` |
| `PUT` | `/api/approach-b/exports/{exportId}/files/multipart/parts?fileName=&uploadId=&partNumber=` | Upload a part (requires `Content-Length` header) |
| `POST` | `/api/approach-b/exports/{exportId}/files/multipart/complete` | Complete S3 multipart upload for a staged file |
| `POST` | `/api/approach-b/exports/{exportId}/complete` | Build the final ZIP synchronously, returns `{ exportId, s3Url }` |

### How the ZIP Build Works

```
POST /complete runs synchronously (ZipByCopyBuilder):
  1. ListObjectsV2  exports/{exportId}/staging/*
  2. GetObjectAttributes in parallel (S3:ZipFanOut, default 8) to retrieve
     CRC32 checksums — zero body data transfer. Falls back to streaming
     CRC32 computation for objects uploaded without ChecksumAlgorithm.CRC32.
  3. InitiateMultipartUpload  exports/{exportId}/final.zip
  4. For each staged file:
     - Files >= 5 MiB: UploadPart for ZIP local file header, then CopyPartAsync
       for the file data (S3→S3, no API host traffic), then data descriptor.
     - Files < 5 MiB: downloaded and included inline in the metadata part.
  5. UploadPart for central directory + EOCD
  6. CompleteMultipartUpload on final.zip
  7. (Optional) DeleteObjects for staging cleanup — fire-and-forget when
     S3:DeleteStagingOnComplete = true; otherwise rely on a lifecycle rule.
```

No bytes touch local disk — `UploadPart` streams the request body straight to S3
(`DisablePayloadSigning=true`), and staging objects are re-streamed from S3
directly into the final ZIP multipart upload.

---

## Companion CLI — `S3ExportUploader`

A small .NET console app under `src/S3ExportUploader` that splits large files into S3-compliant parts (≥ 5 MB) and uploads them through the API using either approach. Files are streamed directly from disk — no intermediate splits are written to local storage.

### Build & run

```bash
dotnet build S3ExportApi.sln

# Approach A — sequential parts into a streaming zip
dotnet run --project src/S3ExportUploader -- \
  --approach a --complete ./big.bin

# Approach B — parallel parts into staged zip, 16 MB parts, 8-way parallelism
dotnet run --project src/S3ExportUploader -- \
  --approach b --part-size 16mb --parallelism 8 --complete a.bin b.bin
```

### Options

| Option | Description |
|---|---|
| `--api <url>` | API base URL (default `http://localhost:5000`) |
| `--approach <a\|b>` | `a` = streaming zip, `b` = staged zip (required) |
| `--user <id>` | userId for Approach A (default `anonymous`) |
| `--export-id <guid>` | reuse an existing export instead of starting a new one |
| `--name <name>` | override remote file name (single-file mode) |
| `--complete` | call `/complete` after uploads finish |
| `--part-size <size>` | multipart part size, e.g. `8mb`, `16mb`, `1gb` (default `8mb`, min `5mb`) |
| `--threshold <size>` | use multipart for files larger than this (default = `--part-size`) |
| `--parallelism <n>` | parallel parts for Approach B (default `4`; A is always sequential) |

The client picks the right endpoint per file: small files go through the single-shot upload, files larger than `--threshold` are split into `--part-size` chunks and uploaded as multipart. Approach A keeps parts strictly sequential (server holds a per-session write lock); Approach B uploads parts concurrently.

---

## Folder-driven CLI — `S3ExportFolderUploader`

A second .NET 8 console app under `src/S3ExportFolderUploader` that takes a folder path, starts an export session, walks every file, uploads it through the right endpoint (single-shot or multipart based on `--threshold`) for either Approach A or B, and optionally finalises the export. Adds **resume across runs** (JSON state file), **retry with exponential backoff** on transient HTTP failures, and **concurrency controls** (`--parallelism` for Approach B).

```bash
# Approach A — sequential streaming upload of a whole folder
dotnet run --project src/S3ExportFolderUploader -- \
    --approach a --folder ./outbox --complete

# Approach B — recursive, 16 MB parts, 8-way parallelism, resumable
dotnet run --project src/S3ExportFolderUploader -- \
    --approach b --folder ./outbox --recursive \
    --part-size 16mb --parallelism 8 --resume --complete
```

See [`src/S3ExportFolderUploader/README.md`](src/S3ExportFolderUploader/README.md) for the full option reference and resume semantics.

---

## Postman Collections

Importable Postman collections for both approaches live under [`docs/postman/`](docs/postman/). See the [README there](docs/postman/README.md) for how to import and the variables to set.

---

## Project Structure

```
S3ExportApi.sln
src/S3ExportApi/
├── S3ExportApi.csproj
├── Program.cs
├── appsettings.json
├── appsettings.Development.json
├── Configuration/
│   └── S3Options.cs                    # S3:BucketName config binding
├── Common/
│   └── S3MultipartUploadStream.cs      # Write-only Stream → S3 multipart parts
├── Storage/
│   └── IExportRepository.cs            # In-memory URL store
├── ApproachA_StreamingZip/
│   ├── ExportSession.cs                # Per-export state (ZipArchive + S3 stream)
│   ├── ExportSessionStore.cs           # ConcurrentDictionary session registry
│   └── ExportsStreamingController.cs   # REST endpoints for Approach A
└── ApproachB_StagedZip/
    ├── ExportZipJob.cs                 # Zip-and-upload synchronous job
    └── ExportsStagedController.cs      # REST endpoints for Approach B
src/S3ExportUploader/
├── S3ExportUploader.csproj
└── Program.cs                          # CLI: splits large files, uploads via approach A or B
src/S3ExportFolderUploader/
├── S3ExportFolderUploader.csproj
├── Program.cs                          # Folder-driven CLI with resume, retry, parallelism
└── README.md
```
