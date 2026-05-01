using Microsoft.Extensions.DependencyInjection;

namespace S3ExportApi.ApproachB_StagedZip;

/// <summary>
/// Background worker that drains <see cref="ExportZipJobQueue"/> and runs
/// <see cref="ExportZipJob"/> for each enqueued export.
/// </summary>
public sealed class ExportZipJobWorker : BackgroundService
{
    private readonly ExportZipJobQueue _queue;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<ExportZipJobWorker> _log;

    public ExportZipJobWorker(
        ExportZipJobQueue queue,
        IServiceScopeFactory scopeFactory,
        ILogger<ExportZipJobWorker> log)
    {
        _queue = queue;
        _scopeFactory = scopeFactory;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var state in _queue.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                state.Status = ExportJobStatus.Running;
                state.StartedAt = DateTimeOffset.UtcNow;

                using var scope = _scopeFactory.CreateScope();
                var job = scope.ServiceProvider.GetRequiredService<ExportZipJob>();
                var url = await job.RunAsync(state.ExportId, stoppingToken);

                state.S3Url = url;
                state.Status = ExportJobStatus.Succeeded;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                state.Status = ExportJobStatus.Failed;
                state.Error = "Server shutting down.";
                throw;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Export zip job failed for {ExportId}", state.ExportId);
                state.Status = ExportJobStatus.Failed;
                state.Error = ex.Message;
            }
            finally
            {
                state.CompletedAt = DateTimeOffset.UtcNow;
            }
        }
    }
}
