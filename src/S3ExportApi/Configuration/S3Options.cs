namespace S3ExportApi.Configuration;

public class S3Options
{
    public string BucketName { get; set; } = "";

    /// <summary>
    /// Maximum number of staged objects to download from S3 concurrently while
    /// building the final zip in Approach B.
    /// </summary>
    public int ZipFanOut { get; set; } = 8;

    /// <summary>
    /// When true, Approach B explicitly deletes staged objects after a successful
    /// zip job. When false (default) cleanup is skipped on the hot path; rely on
    /// an S3 lifecycle rule for the staging/ prefix instead.
    /// </summary>
    public bool DeleteStagingOnComplete { get; set; } = false;
}
