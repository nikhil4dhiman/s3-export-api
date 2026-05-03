using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace S3ExportFolderUploader;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        CliOptions opts;
        try
        {
            opts = CliOptions.Parse(args);
        }
        catch (ArgumentException ex)
        {
            Console.Error.WriteLine($"error: {ex.Message}");
            CliOptions.PrintUsage(Console.Error);
            return 2;
        }

        if (opts.ShowHelp)
        {
            CliOptions.PrintUsage(Console.Out);
            return 0;
        }

        if (opts.Folder is null)
        {
            Console.Error.WriteLine("error: --folder is required.");
            CliOptions.PrintUsage(Console.Error);
            return 2;
        }
        if (!Directory.Exists(opts.Folder))
        {
            Console.Error.WriteLine($"error: folder not found or not a directory: {opts.Folder}");
            return 2;
        }

        // Default state file lives inside the folder so it travels with the data being uploaded.
        var stateFile = opts.StateFile ?? Path.Combine(opts.Folder, ".s3-export-state.json");
        var stateFileFull = Path.GetFullPath(stateFile);

        // Walk the folder, excluding the state file itself.
        var files = EnumerateFiles(opts.Folder, opts.Recursive)
            .Where(p => !PathEquals(p, stateFileFull))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();

        if (files.Count == 0)
        {
            Console.Error.WriteLine($"error: no files found in {opts.Folder}");
            return 2;
        }

        using var http = new HttpClient
        {
            // Large multipart parts can take a while; don't let the default 100s timeout kill us.
            Timeout = Timeout.InfiniteTimeSpan,
            BaseAddress = new Uri(opts.BaseUrl, UriKind.Absolute)
        };
        var retry = new RetryingHttp(http, opts.MaxRetries);

        // Resume state — load existing only if --resume; otherwise start fresh.
        var state = opts.Resume ? ExportState.Load(stateFileFull, opts.Approach) : new ExportState(opts.Approach);
        var stateWriter = new StateWriter(state, stateFileFull);

        IExportClient client = opts.Approach switch
        {
            Approach.A => new ApproachAClient(retry, opts.UserId, opts.PartSize),
            Approach.B => new ApproachBClient(retry, opts.PartSize, opts.Parallelism),
            _ => throw new InvalidOperationException()
        };

        // Start (or reuse) the export session.
        string exportId;
        if (opts.ExportId is not null)
        {
            exportId = opts.ExportId.Value.ToString();
        }
        else if (state.ExportId is not null)
        {
            exportId = state.ExportId;
            Console.WriteLine($"resuming export id: {exportId}");
        }
        else
        {
            exportId = await client.StartAsync();
            state.ExportId = exportId;
            stateWriter.Save();
        }
        Console.WriteLine($"export id: {exportId}");

        var sw = Stopwatch.StartNew();
        try
        {
            foreach (var file in files)
            {
                var name = RemoteNameFor(opts.Folder, file);
                var fileState = state.Files.GetOrAdd(name, _ => new FileState());
                if (fileState.Completed)
                {
                    Console.WriteLine($"skipping {name} (already uploaded)");
                    continue;
                }

                var info = new FileInfo(file);
                Console.WriteLine($"uploading {name} ({Format.Bytes(info.Length)})…");

                if (info.Length <= opts.MultipartThreshold)
                {
                    await client.UploadSmallFileAsync(exportId, name, file);
                }
                else
                {
                    await client.UploadLargeFileAsync(exportId, name, file, fileState, stateWriter.Save);
                }

                fileState.Completed = true;
                fileState.Parts.Clear();
                fileState.UploadId = null;
                stateWriter.Save();
            }
        }
        catch (Exception ex)
        {
            stateWriter.Save();
            Console.Error.WriteLine($"error: {ex.Message}");
            return 1;
        }

        if (opts.Complete)
        {
            Console.WriteLine("completing export…");
            var url = await client.CompleteAsync(exportId);
            Console.WriteLine($"done. s3 url: {url}");
            // Successful completion — the state file is no longer needed.
            try { File.Delete(stateFileFull); } catch { /* best-effort */ }
        }
        else
        {
            Console.WriteLine("upload finished. (use --complete to finalise the export)");
        }

        sw.Stop();
        Console.WriteLine($"total time: {sw.Elapsed:mm\\:ss\\.fff}");
        return 0;
    }

    private static IEnumerable<string> EnumerateFiles(string folder, bool recursive) =>
        Directory.EnumerateFiles(
            folder, "*",
            recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly);

    private static string RemoteNameFor(string folder, string file)
    {
        var rel = Path.GetRelativePath(folder, file);
        // Use POSIX separators so zip entry names are consistent across OSes.
        return rel.Replace(Path.DirectorySeparatorChar, '/').Replace(Path.AltDirectorySeparatorChar, '/');
    }

    private static bool PathEquals(string a, string b)
    {
        var fa = Path.GetFullPath(a);
        var fb = Path.GetFullPath(b);
        var cmp = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return string.Equals(fa, fb, cmp);
    }
}

internal enum Approach { A, B }

// ---------------------------------------------------------------------------
// CLI options + parsing
// ---------------------------------------------------------------------------

internal sealed class CliOptions
{
    public string BaseUrl { get; set; } = "http://localhost:5000";
    public Approach Approach { get; set; } = Approach.A;
    public string? Folder { get; set; }
    public bool Recursive { get; set; }
    public string UserId { get; set; } = "anonymous";
    public Guid? ExportId { get; set; }
    public bool Complete { get; set; }
    public long PartSize { get; set; } = 8L * 1024 * 1024;
    public long MultipartThreshold { get; set; } = 8L * 1024 * 1024;
    public int Parallelism { get; set; } = 4;
    public int MaxRetries { get; set; } = 5;
    public string? StateFile { get; set; }
    public bool Resume { get; set; }
    public bool ShowHelp { get; set; }

    public static CliOptions Parse(string[] args)
    {
        var o = new CliOptions();
        bool partSizeExplicit = false;
        bool thresholdExplicit = false;
        bool approachExplicit = false;

        for (int i = 0; i < args.Length; i++)
        {
            var a = args[i];
            string Next(string flag) =>
                ++i < args.Length ? args[i] : throw new ArgumentException($"missing value for {flag}");

            switch (a)
            {
                case "-h":
                case "--help":
                    o.ShowHelp = true; break;
                case "--api":
                    o.BaseUrl = Next(a); break;
                case "--approach":
                    o.Approach = Next(a).ToLowerInvariant() switch
                    {
                        "a" or "streaming" => Approach.A,
                        "b" or "staged" => Approach.B,
                        var v => throw new ArgumentException($"invalid approach: {v}")
                    };
                    approachExplicit = true;
                    break;
                case "--folder":
                    o.Folder = Next(a); break;
                case "--recursive":
                    o.Recursive = true; break;
                case "--user":
                    o.UserId = Next(a); break;
                case "--export-id":
                    o.ExportId = Guid.Parse(Next(a)); break;
                case "--complete":
                    o.Complete = true; break;
                case "--part-size":
                    o.PartSize = ParseSize(Next(a)); partSizeExplicit = true; break;
                case "--threshold":
                    o.MultipartThreshold = ParseSize(Next(a)); thresholdExplicit = true; break;
                case "--parallelism":
                    o.Parallelism = Math.Max(1, int.Parse(Next(a), CultureInfo.InvariantCulture)); break;
                case "--max-retries":
                    o.MaxRetries = Math.Max(0, int.Parse(Next(a), CultureInfo.InvariantCulture)); break;
                case "--state-file":
                    o.StateFile = Next(a); break;
                case "--resume":
                    o.Resume = true; break;
                default:
                    throw new ArgumentException($"unknown option: {a}");
            }
        }

        if (o.ShowHelp) return o;

        if (!approachExplicit)
            throw new ArgumentException("--approach is required (a or b).");

        if (partSizeExplicit && !thresholdExplicit)
            o.MultipartThreshold = o.PartSize;

        // S3 requires every part except the last to be ≥ 5 MiB.
        const long minPart = 5L * 1024 * 1024;
        if (o.PartSize < minPart)
            throw new ArgumentException($"--part-size must be at least 5 MB (S3 minimum); got {Format.Bytes(o.PartSize)}");

        return o;
    }

    private static long ParseSize(string s)
    {
        s = s.Trim();
        long mult = 1;
        if (EndsWithIgnoreCase(s, "gb")) { mult = 1024L * 1024 * 1024; s = s[..^2]; }
        else if (EndsWithIgnoreCase(s, "mb")) { mult = 1024L * 1024; s = s[..^2]; }
        else if (EndsWithIgnoreCase(s, "kb")) { mult = 1024; s = s[..^2]; }
        else if (EndsWithIgnoreCase(s, "g")) { mult = 1024L * 1024 * 1024; s = s[..^1]; }
        else if (EndsWithIgnoreCase(s, "m")) { mult = 1024L * 1024; s = s[..^1]; }
        else if (EndsWithIgnoreCase(s, "k")) { mult = 1024; s = s[..^1]; }
        else if (EndsWithIgnoreCase(s, "b")) { s = s[..^1]; }
        return (long)(double.Parse(s, CultureInfo.InvariantCulture) * mult);
    }

    private static bool EndsWithIgnoreCase(string s, string suffix) =>
        s.EndsWith(suffix, StringComparison.OrdinalIgnoreCase);

    public static void PrintUsage(TextWriter w)
    {
        w.WriteLine("S3ExportFolderUploader — uploads every file in a folder to the s3-export-api.");
        w.WriteLine();
        w.WriteLine("Usage:");
        w.WriteLine("  S3ExportFolderUploader --approach <a|b> --folder <path> [options]");
        w.WriteLine();
        w.WriteLine("Options:");
        w.WriteLine("  --api <url>             API base url (default: http://localhost:5000)");
        w.WriteLine("  --approach <a|b>        a = streaming zip, b = staged zip (required)");
        w.WriteLine("  --folder <path>         folder whose files will be uploaded (required)");
        w.WriteLine("  --recursive             descend into subfolders (default: top-level only)");
        w.WriteLine("  --user <id>             userId for approach A (default: anonymous)");
        w.WriteLine("  --export-id <guid>      reuse an existing export instead of starting a new one");
        w.WriteLine("  --complete              call /complete after the uploads finish");
        w.WriteLine("  --part-size <size>      multipart part size, e.g. 8mb / 16mb / 1gb (default: 8mb, min: 5mb)");
        w.WriteLine("  --threshold <size>      use multipart for files larger than this (default: part-size)");
        w.WriteLine("  --parallelism <n>       parallel parts for approach B (default: 4; A is always sequential)");
        w.WriteLine("  --max-retries <n>       retries per HTTP request on transient failures (default: 5)");
        w.WriteLine("  --state-file <path>     resume-state file (default: <folder>/.s3-export-state.json)");
        w.WriteLine("  --resume                reuse state file: skip completed files and resume in-progress");
        w.WriteLine("                          multipart uploads where they left off");
        w.WriteLine("  -h, --help              show this message");
        w.WriteLine();
        w.WriteLine("Examples:");
        w.WriteLine("  # Approach A — sequential streaming upload of an entire folder");
        w.WriteLine("  S3ExportFolderUploader --approach a --folder ./outbox --complete");
        w.WriteLine();
        w.WriteLine("  # Approach B — recursive, 16 MB parts, 8-way parallelism, resumable");
        w.WriteLine("  S3ExportFolderUploader --approach b --folder ./outbox --recursive \\");
        w.WriteLine("    --part-size 16mb --parallelism 8 --resume --complete");
    }
}

// ---------------------------------------------------------------------------
// Retry-with-backoff HTTP wrapper. Transient HTTP failures (429, 408, 5xx)
// and network/timeout errors are retried with exponential backoff. The body
// is supplied as a factory so each retry gets a fresh stream from disk.
// ---------------------------------------------------------------------------

internal sealed class RetryingHttp
{
    private static readonly HashSet<HttpStatusCode> RetryableStatuses = new()
    {
        HttpStatusCode.RequestTimeout,         // 408
        (HttpStatusCode)425,                   // Too Early
        (HttpStatusCode)429,                   // Too Many Requests
        HttpStatusCode.InternalServerError,    // 500
        HttpStatusCode.BadGateway,             // 502
        HttpStatusCode.ServiceUnavailable,     // 503
        HttpStatusCode.GatewayTimeout,         // 504
    };

    private readonly HttpClient _http;
    private readonly int _maxRetries;
    private readonly TimeSpan _baseDelay = TimeSpan.FromSeconds(1);
    private readonly TimeSpan _maxDelay = TimeSpan.FromSeconds(30);

    public RetryingHttp(HttpClient http, int maxRetries)
    {
        _http = http;
        _maxRetries = maxRetries;
    }

    public async Task<HttpResponseMessage> SendAsync(
        HttpMethod method, string path, Func<HttpContent>? bodyFactory)
    {
        int attempt = 0;
        while (true)
        {
            attempt++;
            using var req = new HttpRequestMessage(method, path);
            HttpContent? content = bodyFactory?.Invoke();
            if (content is not null) req.Content = content;

            HttpResponseMessage? resp = null;
            try
            {
                resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
                if (resp.IsSuccessStatusCode)
                {
                    return resp;
                }

                if (attempt <= _maxRetries && RetryableStatuses.Contains(resp.StatusCode))
                {
                    var reason = $"{(int)resp.StatusCode} {resp.ReasonPhrase}";
                    resp.Dispose();
                    await BackoffAsync(attempt, reason);
                    continue;
                }

                // Non-retryable or out of retries — surface the error with body.
                var body = await resp.Content.ReadAsStringAsync();
                var statusCode = resp.StatusCode;
                var reasonPhrase = resp.ReasonPhrase;
                resp.Dispose();
                throw new HttpRequestException(
                    $"{(int)statusCode} {reasonPhrase} on {method} {path}: {body}");
            }
            catch (HttpRequestException) when (resp is null && attempt <= _maxRetries)
            {
                // Transport-level failure (DNS, connection refused, connection reset) — retry.
                await BackoffAsync(attempt, "transport error");
                continue;
            }
            catch (TaskCanceledException) when (attempt <= _maxRetries)
            {
                // Treat as timeout — retry.
                await BackoffAsync(attempt, "timeout");
                continue;
            }
            finally
            {
                content?.Dispose();
            }
        }
    }

    private async Task BackoffAsync(int attempt, string reason)
    {
        var delay = TimeSpan.FromMilliseconds(Math.Min(
            _baseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1),
            _maxDelay.TotalMilliseconds));
        Console.WriteLine($"  retry {attempt}/{_maxRetries} after {delay.TotalSeconds:0.#}s ({reason})");
        await Task.Delay(delay);
    }

    public async Task<T?> SendForJsonAsync<T>(HttpMethod method, string path, Func<HttpContent>? bodyFactory)
    {
        using var resp = await SendAsync(method, path, bodyFactory);
        return await resp.Content.ReadFromJsonAsync<T>();
    }
}

// ---------------------------------------------------------------------------
// Read-only stream that exposes [offset, offset+length) of a file. A new file
// handle is opened per construction so concurrent slices of the same file do
// not interfere with each other's positions.
// ---------------------------------------------------------------------------

internal sealed class FileSliceStream : Stream
{
    private readonly FileStream _fs;
    private readonly long _length;
    private long _read;

    public FileSliceStream(string path, long offset, long length)
    {
        _fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
                             bufferSize: 81920, useAsync: true);
        _fs.Position = offset;
        _length = length;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _length;
    public override long Position
    {
        get => _read;
        set => throw new NotSupportedException();
    }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (_read >= _length) return 0;
        int toRead = (int)Math.Min(count, _length - _read);
        int n = _fs.Read(buffer, offset, toRead);
        _read += n;
        return n;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
    {
        if (_read >= _length) return 0;
        int toRead = (int)Math.Min(buffer.Length, _length - _read);
        int n = await _fs.ReadAsync(buffer.Slice(0, toRead), ct);
        _read += n;
        return n;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) _fs.Dispose();
        base.Dispose(disposing);
    }
}

internal static class HttpContentFactory
{
    public static HttpContent ForFileSlice(string path, long offset, long length)
    {
        var slice = new FileSliceStream(path, offset, length);
        var content = new StreamContent(slice);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        content.Headers.ContentLength = length;
        return content;
    }

    public static HttpContent ForJson(object payload) => JsonContent.Create(payload);

    public static HttpContent Empty()
    {
        var c = new ByteArrayContent(Array.Empty<byte>());
        c.Headers.ContentLength = 0;
        return c;
    }
}

// ---------------------------------------------------------------------------
// Resume state — JSON file persisted alongside the source folder by default.
// ---------------------------------------------------------------------------

internal sealed class FileState
{
    [JsonPropertyName("completed")] public bool Completed { get; set; }
    [JsonPropertyName("uploadId")] public string? UploadId { get; set; }
    [JsonPropertyName("parts")] public Dictionary<int, string> Parts { get; set; } = new();
}

internal sealed class ExportState
{
    [JsonPropertyName("approach")] public string Approach { get; set; }
    [JsonPropertyName("exportId")] public string? ExportId { get; set; }
    [JsonPropertyName("files")] public ConcurrentDictionary<string, FileState> Files { get; set; } = new();

    public ExportState() { Approach = "a"; }
    public ExportState(Approach approach) { Approach = approach == S3ExportFolderUploader.Approach.A ? "a" : "b"; }

    public static ExportState Load(string path, Approach approach)
    {
        if (!File.Exists(path)) return new ExportState(approach);
        try
        {
            var json = File.ReadAllText(path);
            var state = JsonSerializer.Deserialize<ExportState>(json);
            if (state is null) return new ExportState(approach);
            // If the saved approach differs from the requested one, the saved exportId
            // is meaningless — start fresh rather than risk mixing approaches.
            var want = approach == S3ExportFolderUploader.Approach.A ? "a" : "b";
            if (!string.Equals(state.Approach, want, StringComparison.OrdinalIgnoreCase))
                return new ExportState(approach);
            return state;
        }
        catch (Exception)
        {
            return new ExportState(approach);
        }
    }

    public void Save(string path)
    {
        var tmp = path + ".tmp";
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);
        var json = JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(tmp, json);
        File.Move(tmp, path, overwrite: true);
    }
}

internal sealed class StateWriter
{
    private readonly ExportState _state;
    private readonly string _path;
    private readonly object _lock = new();

    public StateWriter(ExportState state, string path) { _state = state; _path = path; }

    public void Save()
    {
        lock (_lock) _state.Save(_path);
    }
}

// ---------------------------------------------------------------------------
// Per-approach export clients
// ---------------------------------------------------------------------------

internal interface IExportClient
{
    Task<string> StartAsync();
    Task UploadSmallFileAsync(string exportId, string remoteName, string localPath);
    Task UploadLargeFileAsync(string exportId, string remoteName, string localPath,
                              FileState fileState, Action saveState);
    Task<string> CompleteAsync(string exportId);
}

/// <summary>Approach A — streaming zip. Per-export write lock means parts MUST be sequential.</summary>
internal sealed class ApproachAClient : IExportClient
{
    private readonly RetryingHttp _http;
    private readonly string _userId;
    private readonly long _partSize;

    public ApproachAClient(RetryingHttp http, string userId, long partSize)
    {
        _http = http; _userId = userId; _partSize = partSize;
    }

    public async Task<string> StartAsync()
    {
        var json = await _http.SendForJsonAsync<StartResponse>(
            HttpMethod.Post,
            $"/api/approach-a/exports/start?userId={Uri.EscapeDataString(_userId)}",
            () => HttpContentFactory.Empty());
        return json?.ExportId.ToString() ?? throw new InvalidOperationException("empty start response");
    }

    public async Task UploadSmallFileAsync(string exportId, string remoteName, string localPath)
    {
        var size = new FileInfo(localPath).Length;
        using var resp = await _http.SendAsync(
            HttpMethod.Post,
            $"/api/approach-a/exports/{exportId}/files?fileName={Uri.EscapeDataString(remoteName)}",
            () => HttpContentFactory.ForFileSlice(localPath, 0, size));
    }

    public async Task UploadLargeFileAsync(string exportId, string remoteName, string localPath,
                                           FileState fileState, Action saveState)
    {
        // Approach A's server state is non-resumable across crashes (the in-memory
        // ZipArchive is lost), so we always begin a fresh per-file multipart here.
        var startResp = await _http.SendForJsonAsync<StartFileResponseA>(
            HttpMethod.Post,
            $"/api/approach-a/exports/{exportId}/files/multipart/start",
            () => HttpContentFactory.ForJson(new { fileName = remoteName }))
            ?? throw new InvalidOperationException("empty multipart start response");

        var fileUploadId = startResp.FileUploadId;
        fileState.UploadId = fileUploadId.ToString();
        saveState();

        try
        {
            var size = new FileInfo(localPath).Length;
            long offset = 0;
            int partNo = 1;
            while (offset < size)
            {
                long take = Math.Min(_partSize, size - offset);
                long capturedOffset = offset;
                using var resp = await _http.SendAsync(
                    HttpMethod.Put,
                    $"/api/approach-a/exports/{exportId}/files/multipart/{fileUploadId}/parts",
                    () => HttpContentFactory.ForFileSlice(localPath, capturedOffset, take));

                Console.WriteLine($"  part {partNo} ({Format.Bytes(take)}) ok");
                offset += take;
                partNo++;
            }

            using var done = await _http.SendAsync(
                HttpMethod.Post,
                $"/api/approach-a/exports/{exportId}/files/multipart/{fileUploadId}/complete",
                () => HttpContentFactory.Empty());
        }
        catch
        {
            // Best-effort: try to release the server-side write lock so subsequent
            // files in the same export can still proceed.
            try
            {
                using var _ = await _http.SendAsync(
                    HttpMethod.Post,
                    $"/api/approach-a/exports/{exportId}/files/multipart/{fileUploadId}/complete",
                    () => HttpContentFactory.Empty());
            }
            catch { /* swallow — original exception is more interesting */ }
            throw;
        }
    }

    public async Task<string> CompleteAsync(string exportId)
    {
        var done = await _http.SendForJsonAsync<CompleteResponse>(
            HttpMethod.Post,
            $"/api/approach-a/exports/{exportId}/complete",
            () => HttpContentFactory.Empty());
        return done?.S3Url ?? "";
    }

    private sealed record StartFileResponseA([property: JsonPropertyName("fileUploadId")] Guid FileUploadId);
}

/// <summary>Approach B — staged zip. Multipart parts can be uploaded in parallel and resumed.</summary>
internal sealed class ApproachBClient : IExportClient
{
    private readonly RetryingHttp _http;
    private readonly long _partSize;
    private readonly int _parallelism;

    public ApproachBClient(RetryingHttp http, long partSize, int parallelism)
    {
        _http = http; _partSize = partSize; _parallelism = Math.Max(1, parallelism);
    }

    public async Task<string> StartAsync()
    {
        var json = await _http.SendForJsonAsync<StartResponse>(
            HttpMethod.Post, "/api/approach-b/exports/start",
            () => HttpContentFactory.Empty());
        return json?.ExportId.ToString() ?? throw new InvalidOperationException("empty start response");
    }

    public async Task UploadSmallFileAsync(string exportId, string remoteName, string localPath)
    {
        var size = new FileInfo(localPath).Length;
        using var resp = await _http.SendAsync(
            HttpMethod.Post,
            $"/api/approach-b/exports/{exportId}/files?fileName={Uri.EscapeDataString(remoteName)}",
            () => HttpContentFactory.ForFileSlice(localPath, 0, size));
    }

    public async Task UploadLargeFileAsync(string exportId, string remoteName, string localPath,
                                           FileState fileState, Action saveState)
    {
        string fileUploadId;
        if (!string.IsNullOrEmpty(fileState.UploadId))
        {
            // Resume an in-progress multipart upload from a previous run.
            fileUploadId = fileState.UploadId;
            Console.WriteLine($"  resuming multipart {fileUploadId} " +
                              $"({fileState.Parts.Count} parts already uploaded)");
        }
        else
        {
            var startResp = await _http.SendForJsonAsync<StartFileResponseB>(
                HttpMethod.Post,
                $"/api/approach-b/exports/{exportId}/files/multipart/start",
                () => HttpContentFactory.ForJson(new { fileName = remoteName }))
                ?? throw new InvalidOperationException("empty multipart start response");
            fileUploadId = startResp.FileUploadId;
            fileState.UploadId = fileUploadId;
            fileState.Parts.Clear();
            saveState();
        }

        var size = new FileInfo(localPath).Length;
        var plan = PlanParts(size, _partSize);
        var todo = plan.Where(p => !fileState.Parts.ContainsKey(p.PartNumber)).ToList();

        var saveLock = new object();
        if (todo.Count > 0)
        {
            using var sem = new SemaphoreSlim(_parallelism);
            using var cts = new CancellationTokenSource();
            var tasks = new List<Task>(todo.Count);
            foreach (var p in todo)
            {
                var part = p;
                tasks.Add(Task.Run(async () =>
                {
                    await sem.WaitAsync(cts.Token);
                    try
                    {
                        cts.Token.ThrowIfCancellationRequested();
                        var url = $"/api/approach-b/exports/{exportId}/files/multipart/parts" +
                                  $"?fileName={Uri.EscapeDataString(remoteName)}" +
                                  $"&uploadId={Uri.EscapeDataString(fileUploadId)}" +
                                  $"&partNumber={part.PartNumber}";
                        var partResp = await _http.SendForJsonAsync<UploadPartResponse>(
                            HttpMethod.Put, url,
                            () => HttpContentFactory.ForFileSlice(localPath, part.Offset, part.Length))
                            ?? throw new InvalidOperationException("empty part response");
                        lock (saveLock)
                        {
                            fileState.Parts[part.PartNumber] = partResp.ETag;
                            saveState();
                        }
                        Console.WriteLine($"  part {part.PartNumber}/{plan.Count} ({Format.Bytes(part.Length)}) ok");
                    }
                    finally { sem.Release(); }
                }, cts.Token));
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch
            {
                cts.Cancel();
                // Surface the first failure, but don't lose other tasks' exceptions silently.
                try { await Task.WhenAll(tasks); } catch { /* ignore secondary cancellations */ }
                throw;
            }
        }

        var completeBody = new
        {
            fileName = remoteName,
            uploadId = fileUploadId,
            parts = Enumerable.Range(1, plan.Count).Select(n => new { partNumber = n, eTag = fileState.Parts[n] })
        };
        using var done = await _http.SendAsync(
            HttpMethod.Post,
            $"/api/approach-b/exports/{exportId}/files/multipart/complete",
            () => HttpContentFactory.ForJson(completeBody));
    }

    public async Task<string> CompleteAsync(string exportId)
    {
        var done = await _http.SendForJsonAsync<CompleteResponse>(
            HttpMethod.Post,
            $"/api/approach-b/exports/{exportId}/complete",
            () => HttpContentFactory.Empty());
        if (done is null)
            throw new InvalidOperationException("empty complete response");
        return done.S3Url;
    }

    private static List<PartPlan> PlanParts(long total, long partSize)
    {
        var parts = new List<PartPlan>();
        long off = 0;
        int n = 1;
        while (off < total)
        {
            long len = Math.Min(partSize, total - off);
            parts.Add(new PartPlan(n, off, len));
            off += len;
            n++;
        }
        return parts;
    }

    private readonly record struct PartPlan(int PartNumber, long Offset, long Length);

    private sealed record StartFileResponseB(
        [property: JsonPropertyName("fileUploadId")] string FileUploadId,
        [property: JsonPropertyName("key")] string Key);

    private sealed record UploadPartResponse([property: JsonPropertyName("etag")] string ETag);
}

internal sealed record StartResponse([property: JsonPropertyName("exportId")] Guid ExportId);

internal sealed record CompleteResponse(
    [property: JsonPropertyName("exportId")] Guid ExportId,
    [property: JsonPropertyName("s3Url")] string S3Url);

internal static class Format
{
    private static readonly string[] _units = { "B", "KB", "MB", "GB", "TB" };

    public static string Bytes(long n)
    {
        double v = n;
        int u = 0;
        while (v >= 1024 && u < _units.Length - 1) { v /= 1024; u++; }
        return u == 0 ? $"{n} {_units[u]}" : $"{v:0.0} {_units[u]}";
    }
}
