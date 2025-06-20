using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Producers.Core
{
    public class KafkaProducerStats
    {
        public long TotalMessagesSent { get; set; }
        public long SuccessfulMessages { get; set; }
        public long FailedMessages { get; set; }
        public double SuccessRate => TotalMessagesSent > 0 ? (double)SuccessfulMessages / TotalMessagesSent : 0;
        public TimeSpan AverageLatency { get; set; }
        public TimeSpan MinLatency { get; set; }
        public TimeSpan MaxLatency { get; set; }
        public DateTime LastMessageSent { get; set; }
        public long TotalBytesSent { get; set; }
        public double MessagesPerSecond { get; set; }
    }
}
