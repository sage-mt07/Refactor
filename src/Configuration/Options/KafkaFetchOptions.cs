using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Options
{
    /// <summary>
    /// フェッチオプション
    /// </summary>
    public class KafkaFetchOptions
    {
        public string? ConsumerGroupId { get; set; }
        public int MaxMessages { get; set; } = 1000;
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
        public TopicPartitionOffset? FromOffset { get; set; }
        public TopicPartitionOffset? ToOffset { get; set; }
        public List<TopicPartition>? SpecificPartitions { get; set; }
    }

}
