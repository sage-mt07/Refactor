using Confluent.Kafka;
using System;

namespace KsqlDsl.Messaging.Producers.Core
{

    /// <summary>
    /// メッセージ送信結果
    /// </summary>
    public class KafkaDeliveryResult
    {
        public string Topic { get; set; } = string.Empty;
        public int Partition { get; set; }
        public long Offset { get; set; }
        public DateTime Timestamp { get; set; }
        public PersistenceStatus Status { get; set; }
        public Error? Error { get; set; }
        public TimeSpan Latency { get; set; }
    }


}
