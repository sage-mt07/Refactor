using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Models;
using KsqlDsl.Monitoring.Abstractions.Models;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Diagnostics
{
    /// <summary>
    /// プール診断情報
    /// </summary>
    public class PoolDiagnostics
    {
        public ProducerPoolConfig Configuration { get; set; } = new();
        public int TotalPools { get; set; }
        public int TotalActiveProducers { get; set; }
        public int TotalPooledProducers { get; set; }
        public Dictionary<ProducerKey, PoolMetrics> PoolMetrics { get; set; } = new();
        public Dictionary<string, object> SystemMetrics { get; set; } = new();
    }

}
