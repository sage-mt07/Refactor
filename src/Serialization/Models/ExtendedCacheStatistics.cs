using KsqlDsl.Serialization.Avro.Cache;
using KsqlDsl.Serialization.Avro.Performance;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Serialization.Models
{
    public class ExtendedCacheStatistics
    {
        public CacheStatistics BaseStatistics { get; set; } = new();
        public Dictionary<Type, EntityCacheStatus> EntityStatistics { get; set; } = new();
        public Dictionary<string, PerformanceMetrics> PerformanceMetrics { get; set; } = new();
        public List<SlowOperationRecord> SlowOperations { get; set; } = new();
        public long TotalOperations { get; set; }
        public long SlowOperationsCount { get; set; }
        public double SlowOperationRate { get; set; }
        public DateTime LastMetricsReport { get; set; }
    }
}
