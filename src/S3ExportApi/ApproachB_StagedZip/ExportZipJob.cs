using System.IO.Hashing;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Options;
using S3ExportApi.Common;
using S3ExportApi.Configuration;
using S3ExportApi.Storage;

namespace S3ExportApi.ApproachB_StagedZip;

public class ExportZipJob
{
    private readonly IAmazonS3 _s3;
    private readonly S3Options _options;
    private readonly string _bucket;
    private readonly IExportRepository _repo;
    private readonly ILogger<ExportZipJob> _log;

    public ExportZipJob(IAmazonS3 s3, IOptions<S3Options> opt, IExportRepository repo, ILogger<ExportZipJob> log)
    {
        _s3 = s3;
        _options = opt.Value;
        _bucket = _options.BucketName;
        _repo = repo;
        _log = log;
    }

    public async Task<string> RunAsync(Guid exportId, CancellationToken ct)
    {
        var stagingPrefix = $"exports/{exportId}/staging/";
        var finalKey      = $"exports/{exportId}/final.zip";

        // 1. List staged objects
        var staged = new List<S3Object>();
        string? token = null;
        do
        {
            var page = await _s3.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = _bucket,
                Prefix = stagingPrefix,
                ContinuationToken = token
            }, ct);
            staged.AddRange(page.S3Objects);
            token = page.IsTruncated ? page.NextContinuationToken : null;
        } while (token != null);

        if (staged.Count == 0)
            throw new InvalidOperationException($"No staged files for export {exportId}");

        // 2. Gather CRC32 for each staged file. For objects uploaded with
        //    ChecksumAlgorithm.CRC32, we retrieve it via GetObjectAttributes
        //    (no body download). For objects without a stored checksum, we
        //    fall back to streaming the object to compute CRC32 locally.
        var fanOut = Math.Max(1, _options.ZipFanOut);
        var fileInfos = new StagedFileInfo[staged.Count];

        await Parallel.ForEachAsync(
            staged.Select((obj, idx) => (obj, idx)),
            new ParallelOptions { MaxDegreeOfParallelism = fanOut, CancellationToken = ct },
            async (item, c) =>
            {
                var (obj, idx) = item;
                var name = obj.Key.Substring(stagingPrefix.Length);
                var crc = await GetCrc32Async(obj.Key, obj.Size, c);
                fileInfos[idx] = new StagedFileInfo
                {
                    SourceKey = obj.Key,
                    EntryName = name,
                    Size = obj.Size,
                    Crc32 = crc
                };
            });

        // 3. Build ZIP using UploadPartCopy (no file data through the API host)
        var init = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = _bucket,
            Key = finalKey
        }, ct);

        try
        {
            var builder = new ZipByCopyBuilder(_s3, _bucket, _log);
            var parts = await builder.BuildAsync(
                finalKey, init.UploadId, fileInfos.ToList(), fanOut, ct);

            await _s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = finalKey,
                UploadId = init.UploadId,
                PartETags = parts
            }, ct);

            var url = $"s3://{_bucket}/{finalKey}";
            await _repo.SaveUrlAsync(exportId, url);

            if (_options.DeleteStagingOnComplete)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        foreach (var batch in staged.Chunk(1000))
                        {
                            await _s3.DeleteObjectsAsync(new DeleteObjectsRequest
                            {
                                BucketName = _bucket,
                                Objects = batch.Select(o => new KeyVersion { Key = o.Key }).ToList()
                            }, CancellationToken.None);
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Staging cleanup failed for {ExportId}", exportId);
                    }
                }, CancellationToken.None);
            }

            return url;
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Zip job failed for {ExportId}", exportId);
            await _s3.AbortMultipartUploadAsync(_bucket, finalKey, init.UploadId, ct);
            throw;
        }
    }

    /// <summary>
    /// Retrieve CRC32 for a staged object. First tries GetObjectAttributes
    /// (free if the object was uploaded with ChecksumAlgorithm.CRC32). If that
    /// returns no checksum, falls back to streaming the object through a CRC32 hasher.
    /// </summary>
    private async Task<uint> GetCrc32Async(string key, long size, CancellationToken ct)
    {
        // Try S3 native checksum first (zero data transfer)
        try
        {
            var attrs = await _s3.GetObjectAttributesAsync(new GetObjectAttributesRequest
            {
                BucketName = _bucket,
                Key = key,
                ObjectAttributes = new List<ObjectAttributes> { ObjectAttributes.Checksum }
            }, ct);

            if (attrs.Checksum?.ChecksumCRC32 is { } crc32Base64)
            {
                var bytes = Convert.FromBase64String(crc32Base64);
                return BitConverter.IsLittleEndian
                    ? System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(bytes)
                    : BitConverter.ToUInt32(bytes);
            }

            // For multipart-uploaded objects, S3 stores per-part checksums.
            // We can combine them if ObjectParts is available.
            if (attrs.ObjectParts?.Parts is { Count: > 0 } parts)
            {
                uint combined = 0;
                long offset = 0;
                foreach (var part in parts.OrderBy(p => p.PartNumber))
                {
                    if (part.ChecksumCRC32 is { } partCrc)
                    {
                        var partBytes = Convert.FromBase64String(partCrc);
                        var partVal = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(partBytes);
                        if (offset == 0)
                            combined = partVal;
                        else
                            combined = Crc32Combine.Combine(combined, partVal, part.Size);
                        offset += part.Size;
                    }
                    else
                    {
                        // Part doesn't have CRC32 — fall through to streaming
                        goto StreamFallback;
                    }
                }
                return combined;
            }
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound ||
                                            ex.ErrorCode == "InvalidArgument")
        {
            // GetObjectAttributes not supported or object doesn't have checksum attributes
        }

    StreamFallback:
        // Fallback: stream the object to compute CRC32 (one read, no zip/write overhead)
        _log.LogDebug("Computing CRC32 via streaming for {Key} ({Size} bytes)", key, size);
        using var resp = await _s3.GetObjectAsync(_bucket, key, ct);
        var crc32 = new Crc32();
        var buffer = new byte[81920];
        int read;
        while ((read = await resp.ResponseStream.ReadAsync(buffer, ct)) > 0)
        {
            crc32.Append(buffer.AsSpan(0, read));
        }

        // Crc32.GetCurrentHash() returns bytes in big-endian order
        var hash = new byte[4];
        crc32.GetCurrentHash(hash);
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(hash);
    }
}
