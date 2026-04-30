using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using S3ExportApi.Configuration;

namespace S3ExportApi.ApproachB_StagedZip;

[ApiController]
[Route("api/approach-b/exports")]
public class ExportsStagedController : ControllerBase
{
    private readonly IAmazonS3 _s3;
    private readonly string _bucket;
    private readonly ExportZipJob _zipJob;

    public ExportsStagedController(IAmazonS3 s3, IOptions<S3Options> opt, ExportZipJob zipJob)
    {
        _s3 = s3;
        _bucket = opt.Value.BucketName;
        _zipJob = zipJob;
    }

    /// <summary>Start a new staged export. Stateless — just returns a new exportId.</summary>
    [HttpPost("start")]
    public IActionResult Start()
    {
        return Ok(new { exportId = Guid.NewGuid() });
    }

    /// <summary>Upload a small (single-part) file to staging in S3.</summary>
    [HttpPost("{exportId:guid}/files")]
    [RequestSizeLimit(long.MaxValue)]
    public async Task<IActionResult> UploadSmallFile(Guid exportId, [FromQuery] string fileName, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return BadRequest("fileName query parameter is required.");

        var key = $"exports/{exportId}/staging/{fileName}";
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = _bucket,
            Key = key,
            InputStream = Request.Body,
            AutoCloseStream = false,
            DisablePayloadSigning = true
        }, ct);

        return Ok(new { fileName });
    }

    /// <summary>Start a multipart upload for a large file to staging.</summary>
    [HttpPost("{exportId:guid}/files/multipart/start")]
    public async Task<IActionResult> StartMultipartFile(Guid exportId, [FromBody] StartFileRequest body, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(body.FileName))
            return BadRequest("fileName is required.");

        var key = $"exports/{exportId}/staging/{body.FileName}";
        var init = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = _bucket,
            Key = key
        }, ct);

        return Ok(new { fileUploadId = init.UploadId, key });
    }

    /// <summary>Upload a part of a multipart file to staging.</summary>
    [HttpPut("{exportId:guid}/files/multipart/parts")]
    [RequestSizeLimit(long.MaxValue)]
    public async Task<IActionResult> UploadPart(
        Guid exportId,
        [FromQuery] string fileName,
        [FromQuery] string uploadId,
        [FromQuery] int partNumber,
        CancellationToken ct)
    {
        if (!Request.ContentLength.HasValue)
            return BadRequest("Content-Length header is required.");

        if (string.IsNullOrWhiteSpace(fileName))
            return BadRequest("fileName query parameter is required.");

        if (string.IsNullOrWhiteSpace(uploadId))
            return BadRequest("uploadId query parameter is required.");

        var key = $"exports/{exportId}/staging/{fileName}";

        // The AWS SDK's UploadPartAsync wraps InputStream in PartialWrapperStream,
        // which requires a seekable base stream. ASP.NET's Request.Body is forward-only,
        // so spool the part to a temp file and hand a seekable FileStream to the SDK.
        var tempPath = Path.Combine(Path.GetTempPath(), $"s3part-{Guid.NewGuid():N}.tmp");
        try
        {
            await using (var spool = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write,
                                                    FileShare.None, bufferSize: 81920, useAsync: true))
            {
                await Request.Body.CopyToAsync(spool, ct);
            }

            await using var partStream = new FileStream(tempPath, FileMode.Open, FileAccess.Read,
                                                       FileShare.Read, bufferSize: 81920, useAsync: true);

            var resp = await _s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = _bucket,
                Key = key,
                UploadId = uploadId,
                PartNumber = partNumber,
                PartSize = partStream.Length,
                InputStream = partStream
            }, ct);

            return Ok(new { etag = resp.ETag });
        }
        finally
        {
            try { if (System.IO.File.Exists(tempPath)) System.IO.File.Delete(tempPath); }
            catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>Complete a multipart file upload to staging.</summary>
    [HttpPost("{exportId:guid}/files/multipart/complete")]
    public async Task<IActionResult> CompleteMultipartFile(Guid exportId, [FromBody] CompleteFileRequest body, CancellationToken ct)
    {
        var key = $"exports/{exportId}/staging/{body.FileName}";
        await _s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = _bucket,
            Key = key,
            UploadId = body.UploadId,
            PartETags = body.Parts.Select(p => new PartETag(p.PartNumber, p.ETag)).ToList()
        }, ct);

        return Ok();
    }

    /// <summary>Complete the export: zip all staged files and upload the zip to S3.</summary>
    [HttpPost("{exportId:guid}/complete")]
    public async Task<IActionResult> CompleteExport(Guid exportId, CancellationToken ct)
    {
        var url = await _zipJob.RunAsync(exportId, ct);
        return Ok(new { exportId, s3Url = url });
    }
}

public record StartFileRequest(string FileName);
public record CompletePart(int PartNumber, string ETag);
public record CompleteFileRequest(string FileName, string UploadId, List<CompletePart> Parts);
