using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers.Core
{
    /// <summary>
    /// プールされたConsumer
    /// </summary>
    public class PooledConsumer
    {
        public IConsumer<object, object> Consumer { get; set; } = default!;
        public DateTime CreatedAt { get; set; }
        public DateTime LastUsed { get; set; }
        public int UsageCount { get; set; }
        public bool IsHealthy { get; set; } = true;
        public List<TopicPartition> AssignedPartitions { get; set; } = new();
    }


}
