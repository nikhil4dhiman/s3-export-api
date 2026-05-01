using System.IO.Compression;
using System.Threading.Channels;
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

        var init = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = _bucket,
            Key = finalKey
        }, ct);

        try
        {
            await using var partStream = new S3MultipartUploadStream(_s3, _bucket, finalKey, init.UploadId);

            // Bounded producer/consumer pipeline: N parallel S3 downloads feed a
            // single zip writer. The zip format is inherently sequential, so the
            // writer stays single-threaded; the wins come from overlapping S3
            // GetObject latency and the API->S3 final upload with subsequent
            // downloads.
            var fanOut = Math.Max(1, _options.ZipFanOut);
            var channel = Channel.CreateBounded<StagedDownload>(
                new BoundedChannelOptions(fanOut)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait
                });

            using var producerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

            var producer = Task.Run(async () =>
            {
                try
                {
                    await Parallel.ForEachAsync(staged, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = fanOut,
                        CancellationToken = producerCts.Token
                    }, async (obj, c) =>
                    {
                        var get = await _s3.GetObjectAsync(_bucket, obj.Key, c);
                        var name = obj.Key.Substring(stagingPrefix.Length);
                        await channel.Writer.WriteAsync(new StagedDownload(name, get), c);
                    });
                }
                finally
                {
                    channel.Writer.TryComplete();
                }
            }, producerCts.Token);

            try
            {
                using (var zip = new ZipArchive(partStream, ZipArchiveMode.Create, leaveOpen: true))
                {
                    await foreach (var item in channel.Reader.ReadAllAsync(ct))
                    {
                        ct.ThrowIfCancellationRequested();
                        try
                        {
                            var entry = zip.CreateEntry(item.Name, CompressionLevel.Fastest);
                            await using var entryStream = entry.Open();
                            await item.Response.ResponseStream.CopyToAsync(entryStream, ct);
                        }
                        finally
                        {
                            item.Response.Dispose();
                        }
                    }
                }
            }
            catch
            {
                producerCts.Cancel();
                throw;
            }

            await producer;
            await partStream.FlushFinalAsync(ct);

            await _s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = finalKey,
                UploadId = init.UploadId,
                PartETags = partStream.Parts.ToList()
            }, ct);

            var url = $"s3://{_bucket}/{finalKey}";
            await _repo.SaveUrlAsync(exportId, url);

            if (_options.DeleteStagingOnComplete)
            {
                // Fire-and-forget: cleanup must not delay the caller. Failures
                // are logged and otherwise ignored — staging is reclaimable via
                // an S3 lifecycle rule on the staging/ prefix.
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

    private readonly record struct StagedDownload(string Name, GetObjectResponse Response);
}
