using System.IO.Compression;
using S3ExportApi.Common;

namespace S3ExportApi.ApproachA_StreamingZip;

public sealed class ExportSession : IAsyncDisposable
{
    public required Guid ExportId { get; init; }
    public required string S3Key { get; init; }
    public required string UploadId { get; init; }
    public required S3MultipartUploadStream PartStream { get; init; }
    public required ZipArchive Zip { get; init; }
    public SemaphoreSlim WriteLock { get; } = new(1, 1);
    public Dictionary<Guid, Stream> OpenEntries { get; } = new();

    public async ValueTask DisposeAsync()
    {
        foreach (var stream in OpenEntries.Values) await stream.DisposeAsync();
        Zip.Dispose();
        await PartStream.FlushFinalAsync();
    }
}
