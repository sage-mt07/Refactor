using KsqlDsl.Messaging.Configuration;
using KsqlDsl.Monitoring.Metrics;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Diagnostics
{
    /// <summary>
    /// Producer診断情報
    /// </summary>
    public class ProducerDiagnostics
    {
        public KafkaProducerConfig Configuration { get; set; } = new();
        public ProducerPerformanceStats PerformanceStats { get; set; } = new();
        public PoolDiagnostics PoolDiagnostics { get; set; } = new();
        public Dictionary<Type, ProducerEntityStats> EntityStatistics { get; set; } = new();
        public Dictionary<string, object> SystemMetrics { get; set; } = new();
    }
}
