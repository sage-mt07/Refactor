using Confluent.Kafka;
using System;

namespace KsqlDsl.Core.Abstractions;


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
