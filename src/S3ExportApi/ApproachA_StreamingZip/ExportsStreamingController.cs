using System.IO.Compression;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using S3ExportApi.Common;
using S3ExportApi.Configuration;
using S3ExportApi.Storage;

namespace S3ExportApi.ApproachA_StreamingZip;

[ApiController]
[Route("api/approach-a/exports")]
public class ExportsStreamingController : ControllerBase
{
    private readonly IAmazonS3 _s3;
    private readonly string _bucket;
    private readonly ExportSessionStore _store;
    private readonly IExportRepository _repo;

    public ExportsStreamingController(
        IAmazonS3 s3,
        IOptions<S3Options> opt,
        ExportSessionStore store,
        IExportRepository repo)
    {
        _s3 = s3;
        _bucket = opt.Value.BucketName;
        _store = store;
        _repo = repo;
    }

    /// <summary>Start a new streaming export session.</summary>
    [HttpPost("start")]
    public async Task<IActionResult> Start([FromQuery] string userId = "anonymous")
    {
        var exportId = Guid.NewGuid();
        var key = $"exports/{userId}/{exportId}.zip";

        var init = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = _bucket,
            Key = key
        });

        var partStream = new S3MultipartUploadStream(_s3, _bucket, key, init.UploadId);
        var zip = new ZipArchive(partStream, ZipArchiveMode.Create, leaveOpen: true);

        _store.Add(new ExportSession
        {
            ExportId = exportId,
            S3Key = key,
            UploadId = init.UploadId,
            PartStream = partStream,
            Zip = zip
        });

        return Ok(new { exportId });
    }

    /// <summary>Upload a small (single-part) file into the export zip.</summary>
    [HttpPost("{exportId:guid}/files")]
    [RequestSizeLimit(long.MaxValue)]
    public async Task<IActionResult> UploadSmallFile(Guid exportId, [FromQuery] string fileName, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return BadRequest("fileName query parameter is required.");

        var session = _store.Get(exportId);
        await session.WriteLock.WaitAsync(ct);
        try
        {
            var entry = session.Zip.CreateEntry(fileName, CompressionLevel.Optimal);
            await using var entryStream = entry.Open();
            await Request.Body.CopyToAsync(entryStream, ct);
        }
        finally
        {
            session.WriteLock.Release();
        }

        return Ok();
    }

    /// <summary>Start a multipart file upload into the export zip (acquires write lock).</summary>
    [HttpPost("{exportId:guid}/files/multipart/start")]
    public async Task<IActionResult> StartMultipartFile(Guid exportId, [FromBody] StartMultipartFileRequest body, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(body.FileName))
            return BadRequest("fileName is required.");

        var session = _store.Get(exportId);
        await session.WriteLock.WaitAsync(ct);

        var fileUploadId = Guid.NewGuid();
        var entry = session.Zip.CreateEntry(body.FileName, CompressionLevel.Optimal);
        var entryStream = entry.Open();
        session.OpenEntries[fileUploadId] = entryStream;

        return Ok(new { fileUploadId });
    }

    /// <summary>Append a part to an in-progress multipart file upload.</summary>
    [HttpPut("{exportId:guid}/files/multipart/{fileUploadId:guid}/parts")]
    [RequestSizeLimit(long.MaxValue)]
    public async Task<IActionResult> UploadPart(Guid exportId, Guid fileUploadId, CancellationToken ct)
    {
        var session = _store.Get(exportId);
        if (!session.OpenEntries.TryGetValue(fileUploadId, out var entryStream))
            return NotFound($"File upload {fileUploadId} not found.");

        await Request.Body.CopyToAsync(entryStream, ct);
        return Ok();
    }

    /// <summary>Complete a multipart file upload and release the write lock.</summary>
    [HttpPost("{exportId:guid}/files/multipart/{fileUploadId:guid}/complete")]
    public async Task<IActionResult> CompleteMultipartFile(Guid exportId, Guid fileUploadId)
    {
        var session = _store.Get(exportId);
        if (!session.OpenEntries.TryGetValue(fileUploadId, out var entryStream))
            return NotFound($"File upload {fileUploadId} not found.");

        await entryStream.DisposeAsync();
        session.OpenEntries.Remove(fileUploadId);
        session.WriteLock.Release();

        return Ok();
    }

    /// <summary>Complete the export: close zip, finish multipart upload, return S3 URL.</summary>
    [HttpPost("{exportId:guid}/complete")]
    public async Task<IActionResult> CompleteExport(Guid exportId, CancellationToken ct)
    {
        if (!_store.TryRemove(exportId, out var session) || session is null)
            return NotFound($"Export {exportId} not found.");

        try
        {
            await session.DisposeAsync();

            await _s3.CompleteMultipartUploadAsync(new Amazon.S3.Model.CompleteMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = session.S3Key,
                UploadId = session.UploadId,
                PartETags = session.PartStream.Parts.ToList()
            }, ct);

            var url = $"s3://{_bucket}/{session.S3Key}";
            await _repo.SaveUrlAsync(exportId, url);

            return Ok(new { exportId, s3Url = url });
        }
        catch (Exception)
        {
            await _s3.AbortMultipartUploadAsync(_bucket, session.S3Key, session.UploadId, ct);
            throw;
        }
    }

    /// <summary>Abort and delete an in-progress export.</summary>
    [HttpDelete("{exportId:guid}")]
    public async Task<IActionResult> AbortExport(Guid exportId, CancellationToken ct)
    {
        if (!_store.TryRemove(exportId, out var session) || session is null)
            return NotFound($"Export {exportId} not found.");

        await session.DisposeAsync();
        await _s3.AbortMultipartUploadAsync(_bucket, session.S3Key, session.UploadId, ct);

        return NoContent();
    }
}

public record StartMultipartFileRequest(string FileName);
