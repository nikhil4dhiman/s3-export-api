#!/usr/bin/env python3
"""S3 Export Uploader (Python).

Folder-driven CLI for the s3-export-api. Walks a configured folder, starts an
export session, uploads every file (single-shot or multipart depending on size)
through Approach A (streaming zip) or Approach B (staged zip), and finalises
the export. Supports resume across runs, automatic retry with exponential
backoff on transient failures, and configurable concurrency.

Standard library only — no third-party dependencies. Run with:

    python3 s3_export_uploader.py --help
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

# S3 requires every multipart part except the last to be at least 5 MiB.
S3_MIN_PART_SIZE = 5 * 1024 * 1024
DEFAULT_PART_SIZE = 8 * 1024 * 1024
DEFAULT_PARALLELISM = 4
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_BASE = 1.0  # seconds


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def parse_size(s: str) -> int:
    """Parse strings like '5mb', '16MB', '1g', '512kb' into bytes."""
    s = s.strip().lower()
    mult = 1
    for suffix, m in (("gb", 1024**3), ("g", 1024**3),
                      ("mb", 1024**2), ("m", 1024**2),
                      ("kb", 1024), ("k", 1024),
                      ("b", 1)):
        if s.endswith(suffix):
            mult = m
            s = s[: -len(suffix)]
            break
    return int(float(s) * mult)


def format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n} B"


# ----------------------------------------------------------------------------
# HTTP — minimal, streaming-friendly, with retry/backoff
# ----------------------------------------------------------------------------

class HttpError(Exception):
    """Non-retryable HTTP failure (2xx not received and no retry remaining)."""

    def __init__(self, status: int, method: str, url: str, body: str):
        super().__init__(f"{status} on {method} {url}: {body[:500]}")
        self.status = status
        self.method = method
        self.url = url
        self.body = body


# Status codes worth retrying — transient network/server errors.
_RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in _RETRYABLE_STATUSES
    if isinstance(exc, urllib.error.URLError):
        return True  # DNS / connection / timeout
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    return False


@dataclass
class HttpClient:
    base_url: str
    max_retries: int = DEFAULT_MAX_RETRIES
    backoff_base: float = DEFAULT_BACKOFF_BASE
    timeout: float = 600.0  # seconds per request — large parts can take a while

    def request(
        self,
        method: str,
        path: str,
        *,
        data: bytes | None = None,
        body_provider: Callable[[], Any] | None = None,
        content_length: int | None = None,
        content_type: str = "application/octet-stream",
        accept_json: bool = True,
    ) -> tuple[int, bytes]:
        """Send a request; retry transient failures with exponential backoff.

        For request bodies that are file slices we pass `body_provider` — a
        zero-arg callable that returns a fresh, readable, file-like body. This
        is essential because retries must re-read the slice from the start.
        """
        url = self.base_url.rstrip("/") + path
        attempt = 0
        while True:
            attempt += 1
            body: Any = data
            if body_provider is not None:
                body = body_provider()
            req = urllib.request.Request(url, method=method, data=body)
            if data is not None or body_provider is not None:
                req.add_header("Content-Type", content_type)
                if content_length is not None:
                    req.add_header("Content-Length", str(content_length))
            if accept_json:
                req.add_header("Accept", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return resp.status, resp.read()
            except urllib.error.HTTPError as e:
                payload = e.read() if hasattr(e, "read") else b""
                if _is_retryable(e) and attempt <= self.max_retries:
                    self._sleep_backoff(attempt, f"{e.code} {e.reason}")
                    continue
                raise HttpError(e.code, method, url, payload.decode("utf-8", "replace")) from e
            except Exception as e:  # network / timeout
                if _is_retryable(e) and attempt <= self.max_retries:
                    self._sleep_backoff(attempt, repr(e))
                    continue
                raise

    def _sleep_backoff(self, attempt: int, reason: str) -> None:
        delay = min(self.backoff_base * (2 ** (attempt - 1)), 30.0)
        print(f"  retry {attempt}/{self.max_retries} after {delay:.1f}s ({reason})", flush=True)
        time.sleep(delay)

    # Convenience wrappers ----------------------------------------------------
    def post_json(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps(payload or {}).encode("utf-8") if payload is not None else None
        status, data = self.request(
            "POST", path,
            data=body if body is not None else b"",
            content_length=len(body) if body is not None else 0,
            content_type="application/json" if body is not None else "application/octet-stream",
        )
        return json.loads(data) if data else {}

    def post_empty(self, path: str) -> dict[str, Any]:
        status, data = self.request("POST", path, data=b"", content_length=0)
        return json.loads(data) if data else {}


# ----------------------------------------------------------------------------
# Streaming file slice — reads exactly `length` bytes starting at `offset`.
# Needed because urllib treats any object with .read() as a streaming body and
# we want HttpClient to retransmit the same range on retry without buffering.
# ----------------------------------------------------------------------------

class _FileSlice:
    """Read-only file-like that exposes [offset, offset+length) of `path`.

    A new file handle is opened on construction so that concurrent slices of
    the same path do not interfere with each other's read positions.
    """

    def __init__(self, path: str, offset: int, length: int):
        self._fh = open(path, "rb")
        self._fh.seek(offset)
        self._remaining = length

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if size is None or size < 0 or size > self._remaining:
            size = self._remaining
        chunk = self._fh.read(size)
        self._remaining -= len(chunk)
        return chunk

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass

    def __enter__(self) -> "_FileSlice":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ----------------------------------------------------------------------------
# Resume state — JSON file persisted alongside the source folder by default.
# ----------------------------------------------------------------------------

@dataclass
class FileState:
    completed: bool = False
    # Approach B multipart only:
    upload_id: str | None = None
    parts: dict[int, str] = field(default_factory=dict)  # partNumber -> etag


@dataclass
class ExportState:
    approach: str
    export_id: str | None = None
    files: dict[str, FileState] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path, approach: str) -> "ExportState":
        if not path.exists():
            return cls(approach=approach)
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return cls(approach=approach)
        if data.get("approach") != approach:
            # Different approach — start fresh; the saved exportId would not apply.
            return cls(approach=approach)
        files = {}
        for name, entry in (data.get("files") or {}).items():
            files[name] = FileState(
                completed=bool(entry.get("completed")),
                upload_id=entry.get("upload_id"),
                parts={int(k): v for k, v in (entry.get("parts") or {}).items()},
            )
        return cls(approach=approach, export_id=data.get("export_id"), files=files)

    def save(self, path: Path) -> None:
        payload = {
            "approach": self.approach,
            "export_id": self.export_id,
            "files": {
                name: {
                    "completed": fs.completed,
                    "upload_id": fs.upload_id,
                    "parts": {str(k): v for k, v in fs.parts.items()},
                }
                for name, fs in self.files.items()
            },
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        os.replace(tmp, path)


class _StateWriter:
    """Thread-safe debounced state persister."""

    def __init__(self, state: ExportState, path: Path):
        self._state = state
        self._path = path
        self._lock = threading.Lock()

    def save(self) -> None:
        with self._lock:
            self._state.save(self._path)


# ----------------------------------------------------------------------------
# Approach A — streaming zip. Server holds a per-export write lock so multipart
# parts MUST be uploaded sequentially. Resume granularity is per-file.
# ----------------------------------------------------------------------------

class ApproachAClient:
    def __init__(self, http: HttpClient, user_id: str, part_size: int):
        self.http = http
        self.user_id = user_id
        self.part_size = part_size

    def start(self) -> str:
        resp = self.http.post_empty(
            f"/api/approach-a/exports/start?userId={urllib.parse.quote(self.user_id)}"
        )
        return resp["exportId"]

    def upload_small(self, export_id: str, remote_name: str, local_path: str) -> None:
        size = os.path.getsize(local_path)
        path = (
            f"/api/approach-a/exports/{export_id}/files"
            f"?fileName={urllib.parse.quote(remote_name)}"
        )
        self.http.request(
            "POST", path,
            body_provider=lambda: _FileSlice(local_path, 0, size),
            content_length=size,
        )

    def upload_large(
        self,
        export_id: str,
        remote_name: str,
        local_path: str,
        file_state: FileState,
        save_state: Callable[[], None],
    ) -> None:
        size = os.path.getsize(local_path)
        # Approach A's server state is non-resumable across crashes (the in-memory
        # ZipArchive is lost), so we always start a fresh per-file multipart here.
        start = self.http.post_json(
            f"/api/approach-a/exports/{export_id}/files/multipart/start",
            {"fileName": remote_name},
        )
        file_upload_id = start["fileUploadId"]
        file_state.upload_id = file_upload_id
        save_state()
        try:
            offset = 0
            part_no = 1
            while offset < size:
                length = min(self.part_size, size - offset)
                self.http.request(
                    "PUT",
                    f"/api/approach-a/exports/{export_id}/files/multipart/{file_upload_id}/parts",
                    body_provider=lambda o=offset, l=length: _FileSlice(local_path, o, l),
                    content_length=length,
                )
                print(f"  part {part_no} ({format_bytes(length)}) ok", flush=True)
                offset += length
                part_no += 1
            self.http.post_empty(
                f"/api/approach-a/exports/{export_id}/files/multipart/{file_upload_id}/complete"
            )
        except BaseException:
            # Best effort: release the server-side write lock so subsequent
            # files in the same export can still proceed.
            try:
                self.http.post_empty(
                    f"/api/approach-a/exports/{export_id}/files/multipart/{file_upload_id}/complete"
                )
            except Exception:
                pass
            raise

    def complete(self, export_id: str) -> str:
        resp = self.http.post_empty(f"/api/approach-a/exports/{export_id}/complete")
        return resp.get("s3Url", "")


# ----------------------------------------------------------------------------
# Approach B — staged zip. Each file is an independent S3 object, so multipart
# parts can be uploaded in parallel and a partially-uploaded multipart is fully
# resumable: we keep the uploadId + per-part ETags in the state file and skip
# already-completed parts on a subsequent run.
# ----------------------------------------------------------------------------

class ApproachBClient:
    def __init__(self, http: HttpClient, part_size: int, parallelism: int):
        self.http = http
        self.part_size = part_size
        self.parallelism = max(1, parallelism)

    def start(self) -> str:
        resp = self.http.post_empty("/api/approach-b/exports/start")
        return resp["exportId"]

    def upload_small(self, export_id: str, remote_name: str, local_path: str) -> None:
        size = os.path.getsize(local_path)
        path = (
            f"/api/approach-b/exports/{export_id}/files"
            f"?fileName={urllib.parse.quote(remote_name)}"
        )
        self.http.request(
            "POST", path,
            body_provider=lambda: _FileSlice(local_path, 0, size),
            content_length=size,
        )

    def upload_large(
        self,
        export_id: str,
        remote_name: str,
        local_path: str,
        file_state: FileState,
        save_state: Callable[[], None],
    ) -> None:
        size = os.path.getsize(local_path)

        # Reuse an in-progress multipart upload from a previous run if one exists.
        if file_state.upload_id:
            file_upload_id = file_state.upload_id
            print(f"  resuming multipart {file_upload_id} "
                  f"({len(file_state.parts)} parts already uploaded)", flush=True)
        else:
            start = self.http.post_json(
                f"/api/approach-b/exports/{export_id}/files/multipart/start",
                {"fileName": remote_name},
            )
            file_upload_id = start["fileUploadId"]
            file_state.upload_id = file_upload_id
            file_state.parts = {}
            save_state()

        plan = self._plan_parts(size, self.part_size)
        # Filter out parts whose etag we already have in state.
        todo = [(idx, off, ln) for (idx, off, ln) in plan if (idx + 1) not in file_state.parts]

        save_lock = threading.Lock()

        def upload_one(idx: int, off: int, ln: int) -> None:
            part_no = idx + 1
            url = (
                f"/api/approach-b/exports/{export_id}/files/multipart/parts"
                f"?fileName={urllib.parse.quote(remote_name)}"
                f"&uploadId={urllib.parse.quote(file_upload_id)}"
                f"&partNumber={part_no}"
            )
            _, body = self.http.request(
                "PUT", url,
                body_provider=lambda: _FileSlice(local_path, off, ln),
                content_length=ln,
            )
            etag = json.loads(body)["etag"]
            with save_lock:
                file_state.parts[part_no] = etag
                save_state()
            print(f"  part {part_no}/{len(plan)} ({format_bytes(ln)}) ok", flush=True)

        if todo:
            with ThreadPoolExecutor(max_workers=self.parallelism) as pool:
                futures = [pool.submit(upload_one, idx, off, ln) for (idx, off, ln) in todo]
                # Surface the first exception promptly; cancel pending work.
                first_exc: BaseException | None = None
                for fut in as_completed(futures):
                    try:
                        fut.result()
                    except BaseException as exc:  # noqa: BLE001
                        first_exc = first_exc or exc
                        for pending in futures:
                            pending.cancel()
                if first_exc is not None:
                    raise first_exc

        # Complete the multipart upload with the full ordered etag list.
        complete_payload = {
            "fileName": remote_name,
            "uploadId": file_upload_id,
            "parts": [
                {"partNumber": part_no, "eTag": file_state.parts[part_no]}
                for part_no in range(1, len(plan) + 1)
            ],
        }
        self.http.post_json(
            f"/api/approach-b/exports/{export_id}/files/multipart/complete",
            complete_payload,
        )

    def complete(self, export_id: str) -> str:
        resp = self.http.post_empty(f"/api/approach-b/exports/{export_id}/complete")
        return resp.get("s3Url", "")

    @staticmethod
    def _plan_parts(total: int, part_size: int) -> list[tuple[int, int, int]]:
        plan: list[tuple[int, int, int]] = []
        idx = 0
        off = 0
        while off < total:
            ln = min(part_size, total - off)
            plan.append((idx, off, ln))
            off += ln
            idx += 1
        return plan


# ----------------------------------------------------------------------------
# Driver — folder walk + per-file routing
# ----------------------------------------------------------------------------

@dataclass
class CliOptions:
    api: str
    approach: str
    folder: Path
    user: str
    export_id: str | None
    complete: bool
    part_size: int
    threshold: int
    parallelism: int
    max_retries: int
    state_file: Path
    resume: bool
    recursive: bool
    timeout: float


def iter_files(folder: Path, recursive: bool, exclude: set[Path]) -> list[Path]:
    if recursive:
        candidates = (p for p in folder.rglob("*") if p.is_file())
    else:
        candidates = (p for p in folder.iterdir() if p.is_file())
    return sorted(p for p in candidates if p.resolve() not in exclude)


def remote_name_for(folder: Path, file_path: Path) -> str:
    """Use POSIX-style relative paths so zip entries are consistent across OSes."""
    return file_path.relative_to(folder).as_posix()


def run(opts: CliOptions) -> int:
    if not opts.folder.is_dir():
        print(f"error: folder not found or not a directory: {opts.folder}", file=sys.stderr)
        return 2

    # Exclude the state file from the upload list (it lives inside the folder by default).
    try:
        excluded = {opts.state_file.resolve()}
    except OSError:
        excluded = set()
    files = iter_files(opts.folder, opts.recursive, excluded)
    if not files:
        print(f"error: no files found in {opts.folder}", file=sys.stderr)
        return 2

    state = ExportState.load(opts.state_file, opts.approach) if opts.resume \
        else ExportState(approach=opts.approach)
    writer = _StateWriter(state, opts.state_file)

    http = HttpClient(opts.api, max_retries=opts.max_retries, timeout=opts.timeout)
    if opts.approach == "a":
        client: Any = ApproachAClient(http, opts.user, opts.part_size)
    else:
        client = ApproachBClient(http, opts.part_size, opts.parallelism)

    # Start (or reuse) the export session.
    if opts.export_id:
        export_id = opts.export_id
    elif state.export_id:
        export_id = state.export_id
        print(f"resuming export id: {export_id}", flush=True)
    else:
        export_id = client.start()
        state.export_id = export_id
        writer.save()
    print(f"export id: {export_id}", flush=True)

    started = time.monotonic()
    for file_path in files:
        name = remote_name_for(opts.folder, file_path)
        fs_state = state.files.setdefault(name, FileState())
        if fs_state.completed:
            print(f"skipping {name} (already uploaded)", flush=True)
            continue

        size = file_path.stat().st_size
        print(f"uploading {name} ({format_bytes(size)})…", flush=True)
        try:
            if size <= opts.threshold:
                client.upload_small(export_id, name, str(file_path))
            else:
                client.upload_large(
                    export_id, name, str(file_path), fs_state, writer.save
                )
        except BaseException as exc:  # noqa: BLE001
            print(f"error uploading {name}: {exc}", file=sys.stderr)
            writer.save()
            return 1

        fs_state.completed = True
        # Once a multipart file is finalised the saved parts are no longer useful.
        fs_state.parts.clear()
        fs_state.upload_id = None
        writer.save()

    if opts.complete:
        print("completing export…", flush=True)
        s3_url = client.complete(export_id)
        print(f"done. s3 url: {s3_url}", flush=True)
        # Successful completion — drop the state file.
        try:
            opts.state_file.unlink(missing_ok=True)
        except OSError:
            pass
    else:
        print("upload finished. (use --complete to finalise the export)", flush=True)

    elapsed = time.monotonic() - started
    print(f"total time: {elapsed:.2f}s", flush=True)
    return 0


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="s3_export_uploader.py",
        description=(
            "Upload every file in a folder to the s3-export-api. "
            "Picks single-shot or multipart per file based on size, supports both "
            "Approach A (streaming zip) and Approach B (staged zip), with "
            "resume/retry and configurable concurrency."
        ),
    )
    p.add_argument("--api", default="http://localhost:5000",
                   help="API base URL (default: %(default)s)")
    p.add_argument("--approach", required=True, choices=("a", "b"),
                   help="a = streaming zip, b = staged zip")
    p.add_argument("--folder", required=True, type=Path,
                   help="folder whose files will be uploaded")
    p.add_argument("--recursive", action="store_true",
                   help="descend into subfolders (default: top-level only)")
    p.add_argument("--user", default="anonymous",
                   help="userId for Approach A (default: %(default)s)")
    p.add_argument("--export-id",
                   help="reuse an existing export id instead of starting a new one")
    p.add_argument("--complete", action="store_true",
                   help="call /complete after the uploads finish")
    p.add_argument("--part-size", default=str(DEFAULT_PART_SIZE),
                   help=f"multipart part size, e.g. 8mb / 16mb / 1gb "
                        f"(default: {format_bytes(DEFAULT_PART_SIZE)}, min: 5mb)")
    p.add_argument("--threshold", default=None,
                   help="files larger than this use multipart (default: equal to --part-size)")
    p.add_argument("--parallelism", type=int, default=DEFAULT_PARALLELISM,
                   help="parallel multipart parts for Approach B (default: %(default)s; "
                        "Approach A is always sequential)")
    p.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                   help="retries per HTTP request on transient failures (default: %(default)s)")
    p.add_argument("--timeout", type=float, default=600.0,
                   help="per-request HTTP timeout in seconds (default: %(default)s)")
    p.add_argument("--state-file", type=Path, default=None,
                   help="resume-state file path (default: <folder>/.s3-export-state.json)")
    p.add_argument("--resume", action="store_true",
                   help="reuse state file: skip already-uploaded files and pick up "
                        "in-progress multipart uploads where they left off")
    return p


def parse_args(argv: list[str]) -> CliOptions:
    parser = build_parser()
    ns = parser.parse_args(argv)

    part_size = parse_size(ns.part_size)
    if part_size < S3_MIN_PART_SIZE:
        parser.error(f"--part-size must be at least 5 MB (S3 minimum); got {format_bytes(part_size)}")
    threshold = parse_size(ns.threshold) if ns.threshold else part_size
    state_file = ns.state_file or (ns.folder / ".s3-export-state.json")

    return CliOptions(
        api=ns.api,
        approach=ns.approach,
        folder=ns.folder,
        user=ns.user,
        export_id=ns.export_id,
        complete=ns.complete,
        part_size=part_size,
        threshold=threshold,
        parallelism=max(1, ns.parallelism),
        max_retries=max(0, ns.max_retries),
        state_file=state_file,
        resume=ns.resume,
        recursive=ns.recursive,
        timeout=max(1.0, ns.timeout),
    )


def main(argv: list[str] | None = None) -> int:
    try:
        opts = parse_args(sys.argv[1:] if argv is None else argv)
    except SystemExit as e:
        return int(e.code) if isinstance(e.code, int) else 2
    return run(opts)


if __name__ == "__main__":
    sys.exit(main())
