using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
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
