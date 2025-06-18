using KsqlDsl.Core.Abstractions;
using System;

namespace KsqlDsl.Messaging.Consumers.Core
{
    public class ConsumerInstance
    {
        public ConsumerKey ConsumerKey { get; set; } = default!;
        public PooledConsumer PooledConsumer { get; set; } = default!;
        public DateTime RentedAt { get; set; }
        public bool IsActive { get; set; }
    }
}
