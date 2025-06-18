using KsqlDsl.Core.Models;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Monitoring.Abstractions;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
{

    /// <summary>
    /// プール健全性ステータス
    /// </summary>
    public class PoolHealthStatus
    {
        public PoolStatistics Statistics { get; set; } = new();
        public PoolHealthLevel HealthLevel { get; set; }
        public int TotalPools { get; set; }
        public int TotalActiveProducers { get; set; }
        public int TotalPooledProducers { get; set; }
        public Dictionary<ProducerKey, PoolMetrics> PoolMetrics { get; set; } = new();
        public List<PoolHealthIssue> Issues { get; set; } = new();
        public DateTime LastCheck { get; set; }
    }
}
