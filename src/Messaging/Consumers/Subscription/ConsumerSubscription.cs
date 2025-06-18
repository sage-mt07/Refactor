using KsqlDsl.Configuration.Options;
using KsqlDsl.Core.Models;
using KsqlDsl.Messaging.Abstractions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers.Subscription
{
    /// <summary>
    /// 購読管理情報
    /// </summary>
    public class ConsumerSubscription
    {
        public string Id { get; set; } = string.Empty;
        public Type EntityType { get; set; } = default!;
        public IKafkaConsumer<object> Consumer { get; set; } = default!;
        public Func<object, KafkaMessageContext, Task> Handler { get; set; } = default!;
        public KafkaSubscriptionOptions Options { get; set; } = default!;
        public DateTime StartedAt { get; set; }
        public CancellationTokenSource CancellationTokenSource { get; set; } = new();
    }




}
