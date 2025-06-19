using System;

namespace KsqlDsl.Serialization.Avro.Cache
{
    public class CacheStatistics
    {
        public long TotalRequests { get; set; }
        public long CacheHits { get; set; }
        public long CacheMisses { get; set; }
        public double HitRate => TotalRequests > 0 ? (double)CacheHits / TotalRequests : 0.0;
        public int CachedItemCount { get; set; }
        public DateTime LastAccess { get; set; }
        public DateTime? LastClear { get; set; }
        public TimeSpan Uptime { get; set; }
    }
}
