using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Metrics
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
