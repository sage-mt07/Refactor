using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Models
{

    // =============================================================================
    // Data Transfer Objects - メッセージ・結果・設定の定義
    // =============================================================================

    /// <summary>
    /// Kafkaメッセージラッパー
    /// 設計理由：型安全性とメタデータの統合管理
    /// </summary>
    public class KafkaMessage<T> where T : class
    {
        public T Value { get; set; } = default!;
        public object? Key { get; set; }
        public string Topic { get; set; } = string.Empty;
        public int Partition { get; set; }
        public long Offset { get; set; }
        public DateTime Timestamp { get; set; }
        public Headers? Headers { get; set; }
        public KafkaMessageContext? Context { get; set; }
    }

}
