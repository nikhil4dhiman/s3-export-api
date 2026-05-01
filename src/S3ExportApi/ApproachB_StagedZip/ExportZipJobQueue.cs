using System.Collections.Concurrent;
using System.Threading.Channels;

namespace S3ExportApi.ApproachB_StagedZip;

public enum ExportJobStatus
{
    Queued,
    Running,
    Succeeded,
    Failed
}

public sealed class ExportJobState
{
    public required Guid JobId { get; init; }
    public required Guid ExportId { get; init; }
    public ExportJobStatus Status { get; set; } = ExportJobStatus.Queued;
    public string? S3Url { get; set; }
    public string? Error { get; set; }
    public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? StartedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
}

/// <summary>
/// In-memory queue and state store for Approach B zip jobs.
/// `/complete` enqueues a job and returns immediately; a hosted service drains
/// the channel and runs <see cref="ExportZipJob"/> in the background.
/// </summary>
public sealed class ExportZipJobQueue
{
    private readonly Channel<ExportJobState> _channel =
        Channel.CreateUnbounded<ExportJobState>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false
        });

    private readonly ConcurrentDictionary<Guid, ExportJobState> _byExportId = new();
    private readonly ConcurrentDictionary<Guid, ExportJobState> _byJobId = new();

    public ExportJobState Enqueue(Guid exportId)
    {
        var state = new ExportJobState
        {
            JobId = Guid.NewGuid(),
            ExportId = exportId
        };
        _byExportId[exportId] = state;
        _byJobId[state.JobId] = state;
        if (!_channel.Writer.TryWrite(state))
            throw new InvalidOperationException("Failed to enqueue export zip job.");
        return state;
    }

    public ExportJobState? GetByExportId(Guid exportId)
        => _byExportId.TryGetValue(exportId, out var s) ? s : null;

    public ExportJobState? GetByJobId(Guid jobId)
        => _byJobId.TryGetValue(jobId, out var s) ? s : null;

    public ChannelReader<ExportJobState> Reader => _channel.Reader;
}
