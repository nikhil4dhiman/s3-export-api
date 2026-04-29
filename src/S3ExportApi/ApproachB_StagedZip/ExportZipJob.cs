using System.IO.Compression;
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
    private readonly string _bucket;
    private readonly IExportRepository _repo;
    private readonly ILogger<ExportZipJob> _log;

    public ExportZipJob(IAmazonS3 s3, IOptions<S3Options> opt, IExportRepository repo, ILogger<ExportZipJob> log)
    {
        _s3 = s3;
        _bucket = opt.Value.BucketName;
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
            using (var zip = new ZipArchive(partStream, ZipArchiveMode.Create, leaveOpen: true))
            {
                foreach (var obj in staged)
                {
                    ct.ThrowIfCancellationRequested();
                    var name = obj.Key.Substring(stagingPrefix.Length);
                    var entry = zip.CreateEntry(name, CompressionLevel.Optimal);
                    using var get = await _s3.GetObjectAsync(_bucket, obj.Key, ct);
                    await using var entryStream = entry.Open();
                    await get.ResponseStream.CopyToAsync(entryStream, ct);
                }
            }
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

            try
            {
                foreach (var batch in staged.Chunk(1000))
                {
                    await _s3.DeleteObjectsAsync(new DeleteObjectsRequest
                    {
                        BucketName = _bucket,
                        Objects = batch.Select(o => new KeyVersion { Key = o.Key }).ToList()
                    }, ct);
                }
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Staging cleanup failed for {ExportId}", exportId);
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
}
