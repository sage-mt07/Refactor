using KsqlDsl.Configuration.Options;
using KsqlDsl.Core.Models;
using KsqlDsl.Monitoring.Metrics;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Diagnostics
{
    /// <summary>
    /// Consumerプール診断情報
    /// </summary>
    public class ConsumerPoolDiagnostics
    {
        public ConsumerPoolConfig Configuration { get; set; } = new();
        public int TotalPools { get; set; }
        public int TotalActiveConsumers { get; set; }
        public int TotalPooledConsumers { get; set; }
        public Dictionary<ConsumerKey, PoolMetrics> PoolMetrics { get; set; } = new();
        public Dictionary<string, object> SystemMetrics { get; set; } = new();
    }
}
