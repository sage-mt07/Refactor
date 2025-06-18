using KsqlDsl.Monitoring.Abstractions.Models;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Metrics
{
    public class ExtendedCacheStatistics
    {
        public CacheStatistics BaseStatistics { get; set; } = null!;
        public Dictionary<Type, EntityCacheStatus> EntityStatistics { get; set; } = new();
        public Dictionary<string, PerformanceMetrics> PerformanceMetrics { get; set; } = new();
        public List<SlowOperationRecord> SlowOperations { get; set; } = new();
        public long TotalOperations { get; set; }
        public long SlowOperationsCount { get; set; }
        public double SlowOperationRate { get; set; }
        public DateTime LastMetricsReport { get; set; }
    }
}
