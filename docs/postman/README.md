# Postman collections

Two importable Postman collections, one per approach:

- `ApproachA-StreamingZip.postman_collection.json`
- `ApproachB-StagedZip.postman_collection.json`

## Import

In Postman: **File → Import…** and select either JSON file (or both). Each is a self-contained collection — no environment file is required, since all configuration is exposed as collection variables.

## Variables

Edit the collection variables (top-right of the collection in Postman) before running:

| Variable | Where | Default | What to set |
|---|---|---|---|
| `baseUrl` | both | `http://localhost:5000` | Your running API base URL |
| `userId` | A only | `alice` | Owner used in the S3 key |
| `fileName` | both | `notes.txt` / `big.csv` | Logical name of the file inside the ZIP |
| `partNumber` | B only | `1` | Increment between part uploads |

The other variables (`exportId`, `fileUploadId`, `uploadId`, `etagPart1`, …) are populated automatically by test scripts on each request, so subsequent requests in the flow just work.

## Request order

Run the requests top-to-bottom. The numbered prefix (`1.`, `2.`, …) reflects the intended order. For each large file, repeat steps 3 → 4 (×N parts) → 5; for each small file, repeat step 2.

## Bodies

The single-file and part uploads use Postman's **binary** body type (`Body → binary → Select File`). Pick a local file before sending the request — Postman will set `Content-Length` automatically.

## Approach A vs B in one line

- **A** holds a per-session write lock; uploads must be **serialised** within a single export.
- **B** stages each file/part as an independent S3 object; uploads can be **parallelised** freely. The ZIP is built when you call `Complete export`.
