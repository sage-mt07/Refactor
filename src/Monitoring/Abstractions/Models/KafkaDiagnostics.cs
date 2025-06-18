using KsqlDsl.Monitoring.Diagnostics;
using KsqlDsl.Monitoring.Metrics;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
{
    /// <summary>
    /// Kafka診断情報
    /// </summary>
    public class KafkaDiagnostics
    {
        public DateTime GeneratedAt { get; set; }

        // 新しい統合設定オブジェクト
        public KafkaConfigurationSnapshot Configuration { get; set; } = new();

        public ProducerDiagnostics ProducerDiagnostics { get; set; } = new();
        public ConsumerDiagnostics ConsumerDiagnostics { get; set; } = new();
        public ExtendedCacheStatistics AvroCache { get; set; } = new();
        public Dictionary<string, object> SystemInfo { get; set; } = new();
    }

}
