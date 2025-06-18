using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Metrics;

/// <summary>
/// Consumer統計情報
/// </summary>
public class KafkaConsumerStats
{
    public long TotalMessagesReceived { get; set; }
    public long ProcessedMessages { get; set; }
    public long FailedMessages { get; set; }
    public double SuccessRate => TotalMessagesReceived > 0 ? (double)ProcessedMessages / TotalMessagesReceived : 0;
    public TimeSpan AverageProcessingTime { get; set; }
    public TimeSpan MinProcessingTime { get; set; }
    public TimeSpan MaxProcessingTime { get; set; }
    public DateTime LastMessageReceived { get; set; }
    public long TotalBytesReceived { get; set; }
    public double MessagesPerSecond { get; set; }
    public Dictionary<TopicPartition, long> ConsumerLag { get; set; } = new();
    public List<TopicPartition> AssignedPartitions { get; set; } = new();
}