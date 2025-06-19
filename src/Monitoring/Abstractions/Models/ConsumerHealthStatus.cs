using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
{
    /// <summary>
    /// Consumer健全性ステータス
    /// </summary>
    public class ConsumerHealthStatus
    {
        public ConsumerHealthLevel HealthLevel { get; set; }
        public int ActiveConsumers { get; set; }
        public int ActiveSubscriptions { get; set; }
        public ConsumerPoolHealthStatus PoolHealth { get; set; } = new();
        public ConsumerPerformanceStats PerformanceStats { get; set; } = new();
        public List<ConsumerHealthIssue> Issues { get; set; } = new();
        public DateTime LastCheck { get; set; }
    }
}
