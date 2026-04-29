using System.Collections.Concurrent;

namespace S3ExportApi.ApproachA_StreamingZip;

public class ExportSessionStore
{
    private readonly ConcurrentDictionary<Guid, ExportSession> _sessions = new();

    public void Add(ExportSession s) => _sessions[s.ExportId] = s;

    public ExportSession Get(Guid id) =>
        _sessions.TryGetValue(id, out var s) ? s : throw new KeyNotFoundException($"Export {id} not found");

    public bool TryRemove(Guid id, out ExportSession? s) => _sessions.TryRemove(id, out s);
}
