using KsqlDsl.Core.Models;
using KsqlDsl.Monitoring.Abstractions;
using KsqlDsl.Monitoring.Metrics;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Health
{

    /// <summary>
    /// プール健全性ステータス
    /// </summary>
    public class PoolHealthStatus
    {
        public PoolHealthLevel HealthLevel { get; set; }
        public int TotalPools { get; set; }
        public int TotalActiveProducers { get; set; }
        public int TotalPooledProducers { get; set; }
        public Dictionary<ProducerKey, PoolMetrics> PoolMetrics { get; set; } = new();
        public List<PoolHealthIssue> Issues { get; set; } = new();
        public DateTime LastCheck { get; set; }
    }
}
