using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Models;
using System;

namespace KsqlDsl.Messaging.Core
{
    public class PoolMetrics
    {
        public ProducerKey? ProducerKey { get; set; }
        public ConsumerKey? ConsumerKey { get; set; }
        public long CreatedCount { get; set; }
        public long CreationFailures { get; set; }
        public long RentCount { get; set; }
        public long ReturnCount { get; set; }
        public long DiscardedCount { get; set; }
        public long DisposedCount { get; set; }
        public int ActiveProducers { get; set; }
        public int ActiveConsumers { get; set; }
        public DateTime LastDisposalTime { get; set; }
        public string? LastDisposalReason { get; set; }
        public double FailureRate => CreatedCount > 0 ? (double)CreationFailures / CreatedCount : 0;
    }
}
