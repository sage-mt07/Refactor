using Confluent.Kafka;
using System;

namespace KsqlDsl.Core.Abstractions;


public class KafkaMessage<TValue, TKey>
    where TValue : class
    where TKey : notnull
{
    public TValue Value { get; set; } = default!;
    public TKey Key { get; set; } = default!;  // 必須、null非許可
    public string Topic { get; set; } = string.Empty;
    public int Partition { get; set; }
    public long Offset { get; set; }
    public DateTime Timestamp { get; set; }
    public Headers? Headers { get; set; }
    public KafkaMessageContext? Context { get; set; }
}