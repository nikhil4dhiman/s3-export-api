using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace S3ExportUploader;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        ClientOptions opts;
        try
        {
            opts = ClientOptions.Parse(args);
        }
        catch (ArgumentException ex)
        {
            Console.Error.WriteLine($"error: {ex.Message}");
            ClientOptions.PrintUsage(Console.Error);
            return 2;
        }

        if (opts.ShowHelp)
        {
            ClientOptions.PrintUsage(Console.Out);
            return 0;
        }

        if (opts.Files.Count == 0)
        {
            Console.Error.WriteLine("error: at least one file path is required.");
            ClientOptions.PrintUsage(Console.Error);
            return 2;
        }

        foreach (var f in opts.Files)
        {
            if (!File.Exists(f))
            {
                Console.Error.WriteLine($"error: file not found: {f}");
                return 2;
            }
        }

        using var http = new HttpClient
        {
            // Large multipart parts can take a while; don't let the default 100s timeout kill us.
            Timeout = Timeout.InfiniteTimeSpan,
            BaseAddress = new Uri(opts.BaseUrl, UriKind.Absolute)
        };

        IExportClient client = opts.Approach switch
        {
            Approach.A => new ApproachAClient(http, opts),
            Approach.B => new ApproachBClient(http, opts),
            _ => throw new InvalidOperationException()
        };

        var sw = Stopwatch.StartNew();

        var exportId = opts.ExportId ?? await client.StartAsync();
        Console.WriteLine($"export id: {exportId}");

        foreach (var file in opts.Files)
        {
            var info = new FileInfo(file);
            var name = opts.RemoteName ?? info.Name;
            Console.WriteLine($"uploading {info.Name} ({Format.Bytes(info.Length)}) as \"{name}\"…");

            if (info.Length <= opts.MultipartThreshold)
            {
                await client.UploadSmallFileAsync(exportId, name, file);
            }
            else
            {
                await client.UploadLargeFileAsync(exportId, name, file);
            }
        }

        if (opts.Complete)
        {
            Console.WriteLine("completing export…");
            var url = await client.CompleteAsync(exportId);
            Console.WriteLine($"done. s3 url: {url}");
        }
        else
        {
            Console.WriteLine("upload finished. (use --complete to finalise the export)");
        }

        sw.Stop();
        Console.WriteLine($"total time: {sw.Elapsed:mm\\:ss\\.fff}");
        return 0;
    }
}

internal enum Approach { A, B }

internal sealed class ClientOptions
{
    public string BaseUrl { get; set; } = "http://localhost:5000";
    public Approach Approach { get; set; } = Approach.A;
    public string UserId { get; set; } = "anonymous";
    public Guid? ExportId { get; set; }
    public string? RemoteName { get; set; }
    public bool Complete { get; set; }
    public long PartSize { get; set; } = 8L * 1024 * 1024;          // 8 MB
    public long MultipartThreshold { get; set; } = 8L * 1024 * 1024; // default = part size
    public int Parallelism { get; set; } = 4;                        // approach B only
    public List<string> Files { get; } = new();
    public bool ShowHelp { get; set; }

    public static ClientOptions Parse(string[] args)
    {
        var o = new ClientOptions();
        bool partSizeExplicit = false;
        bool thresholdExplicit = false;

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
                    break;
                case "--user":
                    o.UserId = Next(a); break;
                case "--export-id":
                    o.ExportId = Guid.Parse(Next(a)); break;
                case "--name":
                    o.RemoteName = Next(a); break;
                case "--complete":
                    o.Complete = true; break;
                case "--part-size":
                    o.PartSize = ParseSize(Next(a)); partSizeExplicit = true; break;
                case "--threshold":
                    o.MultipartThreshold = ParseSize(Next(a)); thresholdExplicit = true; break;
                case "--parallelism":
                    o.Parallelism = Math.Max(1, int.Parse(Next(a))); break;
                default:
                    if (a.StartsWith('-')) throw new ArgumentException($"unknown option: {a}");
                    o.Files.Add(a);
                    break;
            }
        }

        if (partSizeExplicit && !thresholdExplicit)
            o.MultipartThreshold = o.PartSize;

        // S3 requires every part except the last to be ≥ 5 MB.
        const long minPart = 5L * 1024 * 1024;
        if (o.PartSize < minPart)
            throw new ArgumentException($"--part-size must be at least 5 MB (S3 minimum); got {Format.Bytes(o.PartSize)}");

        return o;
    }

    private static long ParseSize(string s)
    {
        s = s.Trim();
        long mult = 1;
        if (s.EndsWith("kb", StringComparison.OrdinalIgnoreCase) || s.EndsWith("k", StringComparison.OrdinalIgnoreCase))
        { mult = 1024; s = s[..^(s.EndsWith("kb", StringComparison.OrdinalIgnoreCase) ? 2 : 1)]; }
        else if (s.EndsWith("mb", StringComparison.OrdinalIgnoreCase) || s.EndsWith("m", StringComparison.OrdinalIgnoreCase))
        { mult = 1024L * 1024; s = s[..^(s.EndsWith("mb", StringComparison.OrdinalIgnoreCase) ? 2 : 1)]; }
        else if (s.EndsWith("gb", StringComparison.OrdinalIgnoreCase) || s.EndsWith("g", StringComparison.OrdinalIgnoreCase))
        { mult = 1024L * 1024 * 1024; s = s[..^(s.EndsWith("gb", StringComparison.OrdinalIgnoreCase) ? 2 : 1)]; }
        return (long)(double.Parse(s, System.Globalization.CultureInfo.InvariantCulture) * mult);
    }

    public static void PrintUsage(TextWriter w)
    {
        w.WriteLine("S3ExportUploader — splits large files and uploads them to the s3-export-api.");
        w.WriteLine();
        w.WriteLine("Usage:");
        w.WriteLine("  S3ExportUploader --approach <a|b> [options] <file> [<file> ...]");
        w.WriteLine();
        w.WriteLine("Options:");
        w.WriteLine("  --api <url>             API base url (default: http://localhost:5000)");
        w.WriteLine("  --approach <a|b>        a = streaming zip, b = staged zip (required)");
        w.WriteLine("  --user <id>             userId for approach A (default: anonymous)");
        w.WriteLine("  --export-id <guid>      reuse an existing export instead of starting a new one");
        w.WriteLine("  --name <name>           override the remote file name (only when uploading a single file)");
        w.WriteLine("  --complete              call /complete after the uploads finish");
        w.WriteLine("  --part-size <size>      multipart part size, e.g. 8mb / 16mb / 1gb (default: 8mb, min: 5mb)");
        w.WriteLine("  --threshold <size>      use multipart for files larger than this (default: part-size)");
        w.WriteLine("  --parallelism <n>       parallel parts for approach B (default: 4; approach A is always sequential)");
        w.WriteLine("  -h, --help              show this message");
        w.WriteLine();
        w.WriteLine("Examples:");
        w.WriteLine("  # one-shot streaming upload of a 5 GB file");
        w.WriteLine("  S3ExportUploader --approach a --complete ./big.bin");
        w.WriteLine();
        w.WriteLine("  # staged upload with 16 MB parts and 8-way parallelism");
        w.WriteLine("  S3ExportUploader --approach b --part-size 16mb --parallelism 8 --complete a.bin b.bin");
    }
}

internal interface IExportClient
{
    Task<Guid> StartAsync();
    Task UploadSmallFileAsync(Guid exportId, string remoteName, string localPath);
    Task UploadLargeFileAsync(Guid exportId, string remoteName, string localPath);
    Task<string> CompleteAsync(Guid exportId);
}

/// <summary>Approach A — streaming zip. Per-export write lock means parts MUST be sequential.</summary>
internal sealed class ApproachAClient : IExportClient
{
    private readonly HttpClient _http;
    private readonly ClientOptions _opts;

    public ApproachAClient(HttpClient http, ClientOptions opts) { _http = http; _opts = opts; }

    public async Task<Guid> StartAsync()
    {
        using var resp = await _http.PostAsync($"/api/approach-a/exports/start?userId={Uri.EscapeDataString(_opts.UserId)}", content: null);
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadFromJsonAsync<StartResponse>() ?? throw new InvalidOperationException("empty start response");
        return json.ExportId;
    }

    public async Task UploadSmallFileAsync(Guid exportId, string remoteName, string localPath)
    {
        await using var fs = File.OpenRead(localPath);
        using var content = new StreamContent(fs);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        content.Headers.ContentLength = fs.Length;

        using var resp = await _http.PostAsync(
            $"/api/approach-a/exports/{exportId}/files?fileName={Uri.EscapeDataString(remoteName)}",
            content);
        await Http.EnsureSuccess(resp);
    }

    public async Task UploadLargeFileAsync(Guid exportId, string remoteName, string localPath)
    {
        // Start
        var startBody = JsonContent.Create(new { fileName = remoteName });
        using var startResp = await _http.PostAsync(
            $"/api/approach-a/exports/{exportId}/files/multipart/start", startBody);
        await Http.EnsureSuccess(startResp);
        var start = await startResp.Content.ReadFromJsonAsync<StartFileResponseA>()
                    ?? throw new InvalidOperationException("empty multipart start response");

        try
        {
            // Sequential parts (server holds the write lock and appends to a single ZIP entry stream).
            await using var fs = File.OpenRead(localPath);
            long remaining = fs.Length;
            int partNo = 1;
            while (remaining > 0)
            {
                long take = Math.Min(_opts.PartSize, remaining);
                using var slice = new BoundedStream(fs, take);
                using var content = new StreamContent(slice);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                content.Headers.ContentLength = take;

                using var partResp = await _http.PutAsync(
                    $"/api/approach-a/exports/{exportId}/files/multipart/{start.FileUploadId}/parts",
                    content);
                await Http.EnsureSuccess(partResp);

                Console.WriteLine($"  part {partNo} ({Format.Bytes(take)}) ok");
                remaining -= take;
                partNo++;
            }

            using var done = await _http.PostAsync(
                $"/api/approach-a/exports/{exportId}/files/multipart/{start.FileUploadId}/complete",
                content: null);
            await Http.EnsureSuccess(done);
        }
        catch
        {
            // Best-effort: try to release the server-side write lock by completing.
            try
            {
                using var rec = await _http.PostAsync(
                    $"/api/approach-a/exports/{exportId}/files/multipart/{start.FileUploadId}/complete",
                    content: null);
            }
            catch { /* swallow — original exception is more interesting */ }
            throw;
        }
    }

    public async Task<string> CompleteAsync(Guid exportId)
    {
        using var resp = await _http.PostAsync($"/api/approach-a/exports/{exportId}/complete", content: null);
        await Http.EnsureSuccess(resp);
        var done = await resp.Content.ReadFromJsonAsync<CompleteResponse>() ?? throw new InvalidOperationException("empty complete response");
        return done.S3Url;
    }

    private sealed record StartFileResponseA([property: JsonPropertyName("fileUploadId")] Guid FileUploadId);
}

/// <summary>Approach B — staged zip. Per-file multipart parts can be uploaded in parallel.</summary>
internal sealed class ApproachBClient : IExportClient
{
    private readonly HttpClient _http;
    private readonly ClientOptions _opts;

    public ApproachBClient(HttpClient http, ClientOptions opts) { _http = http; _opts = opts; }

    public async Task<Guid> StartAsync()
    {
        using var resp = await _http.PostAsync("/api/approach-b/exports/start", content: null);
        await Http.EnsureSuccess(resp);
        var json = await resp.Content.ReadFromJsonAsync<StartResponse>() ?? throw new InvalidOperationException("empty start response");
        return json.ExportId;
    }

    public async Task UploadSmallFileAsync(Guid exportId, string remoteName, string localPath)
    {
        await using var fs = File.OpenRead(localPath);
        using var content = new StreamContent(fs);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        content.Headers.ContentLength = fs.Length;

        using var resp = await _http.PostAsync(
            $"/api/approach-b/exports/{exportId}/files?fileName={Uri.EscapeDataString(remoteName)}",
            content);
        await Http.EnsureSuccess(resp);
    }

    public async Task UploadLargeFileAsync(Guid exportId, string remoteName, string localPath)
    {
        var startBody = JsonContent.Create(new { fileName = remoteName });
        using var startResp = await _http.PostAsync(
            $"/api/approach-b/exports/{exportId}/files/multipart/start", startBody);
        await Http.EnsureSuccess(startResp);
        var start = await startResp.Content.ReadFromJsonAsync<StartFileResponseB>()
                    ?? throw new InvalidOperationException("empty multipart start response");

        var fileLen = new FileInfo(localPath).Length;
        var parts = PlanParts(fileLen, _opts.PartSize);
        var etags = new string[parts.Count];

        try
        {
            using var sem = new SemaphoreSlim(_opts.Parallelism);
            var tasks = new List<Task>(parts.Count);
            for (int i = 0; i < parts.Count; i++)
            {
                int idx = i;
                var (offset, length) = parts[i];
                tasks.Add(Task.Run(async () =>
                {
                    await sem.WaitAsync();
                    try
                    {
                        // Each part needs its own FileStream so seeks/reads don't interfere.
                        await using var fs = new FileStream(localPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                                                           bufferSize: 81920, useAsync: true);
                        fs.Position = offset;
                        using var slice = new BoundedStream(fs, length);
                        using var content = new StreamContent(slice);
                        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                        content.Headers.ContentLength = length;

                        var url = $"/api/approach-b/exports/{exportId}/files/multipart/parts" +
                                  $"?fileName={Uri.EscapeDataString(remoteName)}" +
                                  $"&uploadId={Uri.EscapeDataString(start.FileUploadId)}" +
                                  $"&partNumber={idx + 1}";
                        using var resp = await _http.PutAsync(url, content);
                        await Http.EnsureSuccess(resp);
                        var partJson = await resp.Content.ReadFromJsonAsync<UploadPartResponse>()
                                       ?? throw new InvalidOperationException("empty part response");
                        etags[idx] = partJson.ETag;
                        Console.WriteLine($"  part {idx + 1}/{parts.Count} ({Format.Bytes(length)}) ok");
                    }
                    finally { sem.Release(); }
                }));
            }
            await Task.WhenAll(tasks);

            var completeBody = JsonContent.Create(new
            {
                fileName = remoteName,
                uploadId = start.FileUploadId,
                parts = Enumerable.Range(0, parts.Count).Select(i => new { partNumber = i + 1, eTag = etags[i] })
            });
            using var done = await _http.PostAsync(
                $"/api/approach-b/exports/{exportId}/files/multipart/complete", completeBody);
            await Http.EnsureSuccess(done);
        }
        catch
        {
            // The server gave us back the S3 UploadId; the client has no abort endpoint, so we can only let
            // S3's lifecycle policy clean up. Re-throw so the user sees the failure.
            throw;
        }
    }

    public async Task<string> CompleteAsync(Guid exportId)
    {
        using var resp = await _http.PostAsync($"/api/approach-b/exports/{exportId}/complete", content: null);
        await Http.EnsureSuccess(resp);

        // /complete is async: it returns 202 with a jobId. Poll the status
        // endpoint until the background zip job finishes.
        var queued = await resp.Content.ReadFromJsonAsync<CompleteAcceptedResponse>()
            ?? throw new InvalidOperationException("empty complete response");

        var pollDelay = TimeSpan.FromMilliseconds(500);
        var maxDelay = TimeSpan.FromSeconds(5);
        while (true)
        {
            using var statusResp = await _http.GetAsync($"/api/approach-b/exports/{exportId}/status");
            await Http.EnsureSuccess(statusResp);
            var status = await statusResp.Content.ReadFromJsonAsync<JobStatusResponse>()
                ?? throw new InvalidOperationException("empty status response");

            switch (status.Status)
            {
                case "succeeded":
                    return status.S3Url ?? "";
                case "failed":
                    throw new InvalidOperationException(
                        $"Export zip job failed: {status.Error ?? "unknown error"}");
                default:
                    await Task.Delay(pollDelay);
                    if (pollDelay < maxDelay)
                        pollDelay = TimeSpan.FromMilliseconds(Math.Min(maxDelay.TotalMilliseconds, pollDelay.TotalMilliseconds * 1.5));
                    break;
            }
        }
    }

    private static List<(long offset, long length)> PlanParts(long total, long partSize)
    {
        var parts = new List<(long, long)>();
        long off = 0;
        while (off < total)
        {
            long len = Math.Min(partSize, total - off);
            parts.Add((off, len));
            off += len;
        }
        return parts;
    }

    private sealed record StartFileResponseB(
        [property: JsonPropertyName("fileUploadId")] string FileUploadId,
        [property: JsonPropertyName("key")] string Key);

    private sealed record UploadPartResponse([property: JsonPropertyName("etag")] string ETag);
}

internal sealed record StartResponse([property: JsonPropertyName("exportId")] Guid ExportId);
internal sealed record CompleteResponse(
    [property: JsonPropertyName("exportId")] Guid ExportId,
    [property: JsonPropertyName("s3Url")] string S3Url);

internal sealed record CompleteAcceptedResponse(
    [property: JsonPropertyName("exportId")] Guid ExportId,
    [property: JsonPropertyName("jobId")] Guid JobId,
    [property: JsonPropertyName("status")] string Status);

internal sealed record JobStatusResponse(
    [property: JsonPropertyName("exportId")] Guid ExportId,
    [property: JsonPropertyName("jobId")] Guid JobId,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("s3Url")] string? S3Url,
    [property: JsonPropertyName("error")] string? Error);

/// <summary>
/// Read-only forward stream that exposes exactly <see cref="_length"/> bytes from the
/// current position of an underlying seekable stream. Used to hand a slice of a file
/// to <see cref="StreamContent"/> without copying it into memory.
/// </summary>
internal sealed class BoundedStream : Stream
{
    private readonly Stream _inner;
    private readonly long _length;
    private long _read;

    public BoundedStream(Stream inner, long length) { _inner = inner; _length = length; }

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
        int n = _inner.Read(buffer, offset, toRead);
        _read += n;
        return n;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_read >= _length) return 0;
        int toRead = (int)Math.Min(buffer.Length, _length - _read);
        int n = await _inner.ReadAsync(buffer.Slice(0, toRead), cancellationToken);
        _read += n;
        return n;
    }
}

internal static class Http
{
    public static async Task EnsureSuccess(HttpResponseMessage resp)
    {
        if (resp.IsSuccessStatusCode) return;
        var body = await resp.Content.ReadAsStringAsync();
        throw new HttpRequestException(
            $"{(int)resp.StatusCode} {resp.ReasonPhrase} on {resp.RequestMessage?.Method} {resp.RequestMessage?.RequestUri}: {body}");
    }
}

internal static class Format
{
    public static string Bytes(long n)
    {
        string[] units = { "B", "KB", "MB", "GB", "TB" };
        double v = n;
        int u = 0;
        while (v >= 1024 && u < units.Length - 1) { v /= 1024; u++; }
        return $"{v:0.##} {units[u]}";
    }
}
