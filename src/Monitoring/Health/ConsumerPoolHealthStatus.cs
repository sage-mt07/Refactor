using KsqlDsl.Core.Models;
using KsqlDsl.Monitoring.Abstractions;
using KsqlDsl.Monitoring.Metrics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Health
{

    /// <summary>
    /// Consumerプール健全性ステータス
    /// </summary>
    public class ConsumerPoolHealthStatus
    {
        public ConsumerPoolHealthLevel HealthLevel { get; set; }
        public int TotalPools { get; set; }
        public int TotalActiveConsumers { get; set; }
        public int TotalPooledConsumers { get; set; }
        public Dictionary<ConsumerKey, PoolMetrics> PoolMetrics { get; set; } = new();
        public List<ConsumerPoolHealthIssue> Issues { get; set; } = new();
        public DateTime LastCheck { get; set; }
    }
}
