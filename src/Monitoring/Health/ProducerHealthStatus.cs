using KsqlDsl.Monitoring.Abstractions;
using KsqlDsl.Monitoring.Metrics;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Health
{
    /// <summary>
    /// Producer健全性ステータス
    /// </summary>
    public class ProducerHealthStatus
    {
        public ProducerHealthLevel HealthLevel { get; set; }
        public int ActiveProducers { get; set; }
        public PoolHealthStatus PoolHealth { get; set; } = new();
        public ProducerPerformanceStats PerformanceStats { get; set; } = new();
        public List<ProducerHealthIssue> Issues { get; set; } = new();
        public DateTime LastCheck { get; set; }
    }
}
