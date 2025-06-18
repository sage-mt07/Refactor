using KsqlDsl.Configuration.Abstractions;
using System;

namespace KsqlDsl.Messaging.Consumers.Subscription
{

    /// <summary>
    /// 購読情報（診断用）
    /// </summary>
    public class SubscriptionInfo
    {
        public string Id { get; set; } = string.Empty;
        public Type EntityType { get; set; } = default!;
        public DateTime StartedAt { get; set; }
        public KafkaSubscriptionOptions Options { get; set; } = default!;
    }

}
