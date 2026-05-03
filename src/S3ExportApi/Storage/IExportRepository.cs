using System.Collections.Concurrent;

namespace S3ExportApi.Storage;

public interface IExportRepository
{
    Task SaveUrlAsync(Guid exportId, string url);
    Task<string?> GetUrlAsync(Guid exportId);
}

public class InMemoryExportRepository : IExportRepository
{
    private readonly ConcurrentDictionary<Guid, string> _store = new();

    public Task SaveUrlAsync(Guid exportId, string url)
    {
        _store[exportId] = url;
        return Task.CompletedTask;
    }

    public Task<string?> GetUrlAsync(Guid exportId)
        => Task.FromResult(_store.TryGetValue(exportId, out var u) ? u : null);
}
