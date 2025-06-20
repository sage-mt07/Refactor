namespace KsqlDsl.Core.Abstractions
{
    public interface ICacheStatistics
    {
        long TotalRequests { get; }
        long CacheHits { get; }
        double HitRate { get; }
    }
}
