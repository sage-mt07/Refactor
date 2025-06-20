using KsqlDsl.Core.Abstractions;

namespace KsqlDsl.Monitoring.Performance
{
    public class PerformanceCacheStatistics : ICacheStatistics
    {
        public long TotalRequests { get; set; }
        public long CacheHits { get; set; }
        public long SlowOperationsCount { get; set; }
        public double SlowOperationRate { get; set; }
        public double HitRate => TotalRequests > 0 ? (double)CacheHits / TotalRequests : 0.0;
    }
}
