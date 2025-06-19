using KsqlDsl.Monitoring.Metrics;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
{
    public class CacheHealthReport
    {
        public DateTime GeneratedAt { get; set; }
        public CacheStatistics GlobalStats { get; set; } = null!;
        public List<EntityCacheStatus> EntityStats { get; set; } = new();
        public List<CacheIssue> Issues { get; set; } = new();
        public CacheHealthLevel HealthLevel { get; set; }
        public List<string> Recommendations { get; set; } = new();
    }
}
