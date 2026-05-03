using Amazon.S3;
using Amazon.S3.Model;

namespace S3ExportApi.Common;

public sealed class S3MultipartUploadStream : Stream
{
    private const int PartSize = 5 * 1024 * 1024;
    private readonly IAmazonS3 _s3;
    private readonly string _bucket;
    private readonly string _key;
    private readonly string _uploadId;
    private readonly List<PartETag> _parts = new();
    private MemoryStream _buffer = new();
    private int _partNumber = 1;
    private bool _finalFlushed;

    public S3MultipartUploadStream(IAmazonS3 s3, string bucket, string key, string uploadId)
    { _s3 = s3; _bucket = bucket; _key = key; _uploadId = uploadId; }

    public IReadOnlyList<PartETag> Parts => _parts;

    public override bool CanWrite => true;
    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override void Write(byte[] buffer, int offset, int count)
        => WriteAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
    {
        await _buffer.WriteAsync(buffer.AsMemory(offset, count), ct);
        while (_buffer.Length >= PartSize) await FlushPartAsync(PartSize, ct);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> source, CancellationToken ct = default)
    {
        await _buffer.WriteAsync(source, ct);
        while (_buffer.Length >= PartSize) await FlushPartAsync(PartSize, ct);
    }

    public async Task FlushFinalAsync(CancellationToken ct = default)
    {
        if (_finalFlushed) return;
        if (_buffer.Length > 0) await FlushPartAsync((int)_buffer.Length, ct);
        _finalFlushed = true;
    }

    private async Task FlushPartAsync(int size, CancellationToken ct)
    {
        var bytes = _buffer.GetBuffer();
        using var partStream = new MemoryStream(bytes, 0, size, writable: false, publiclyVisible: false);
        var resp = await _s3.UploadPartAsync(new UploadPartRequest
        {
            BucketName = _bucket, Key = _key, UploadId = _uploadId,
            PartNumber = _partNumber, PartSize = size, InputStream = partStream
        }, ct);
        _parts.Add(new PartETag(_partNumber, resp.ETag));
        _partNumber++;

        var remainderLen = (int)_buffer.Length - size;
        var newBuffer = new MemoryStream();
        if (remainderLen > 0) await newBuffer.WriteAsync(bytes.AsMemory(size, remainderLen), ct);
        _buffer.Dispose();
        _buffer = newBuffer;
    }

    public override void Flush() { }
    public override int Read(byte[] b, int o, int c) => throw new NotSupportedException();
    public override long Seek(long o, SeekOrigin l) => throw new NotSupportedException();
    public override void SetLength(long v) => throw new NotSupportedException();

    public override async ValueTask DisposeAsync()
    {
        await FlushFinalAsync();
        _buffer.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        // NOTE: This path does NOT flush remaining buffered bytes to S3.
        // Always use DisposeAsync() (e.g. via 'await using') to ensure
        // the final S3 part is uploaded before the stream is released.
        if (disposing) _buffer.Dispose();
        base.Dispose(disposing);
    }
}
