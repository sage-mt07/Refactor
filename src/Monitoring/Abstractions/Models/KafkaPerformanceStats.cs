using KsqlDsl.Monitoring.Metrics;

namespace KsqlDsl.Monitoring.Abstractions.Models
{
    /// <summary>
    /// 統合パフォーマンス統計
    /// </summary>
    public class KafkaPerformanceStats
    {
        public ProducerPerformanceStats ProducerStats { get; set; } = new();
        public ConsumerPerformanceStats ConsumerStats { get; set; } = new();
        public ExtendedCacheStatistics AvroStats { get; set; } = new(); // 既存Avro統計活用
    }


}
