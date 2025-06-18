using KsqlDsl.Monitoring.Abstractions;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
{

    /// <summary>
    /// Kafka全体ヘルス報告
    /// </summary>
    public class KafkaHealthReport
    {
        public DateTime GeneratedAt { get; set; }
        public KafkaHealthLevel HealthLevel { get; set; }

        // コンポーネント別ヘルス
        public ProducerHealthStatus ProducerHealth { get; set; } = new();
        public ConsumerHealthStatus ConsumerHealth { get; set; } = new();
        public CacheHealthReport AvroHealth { get; set; } = new(); // 既存Avro活用

        // 統計情報
        public KafkaPerformanceStats PerformanceStats { get; set; } = new();
        public List<KafkaHealthIssue> Issues { get; set; } = new();
        public List<string> Recommendations { get; set; } = new();
    }

}
