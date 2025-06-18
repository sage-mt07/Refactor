using KsqlDsl.Messaging.Configuration;
using KsqlDsl.Messaging.Consumers.Subscription;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
{

    /// <summary>
    /// Consumer診断情報
    /// </summary>
    public class ConsumerDiagnostics
    {
        public KafkaConsumerConfig Configuration { get; set; } = new();
        public ConsumerPerformanceStats PerformanceStats { get; set; } = new();
        public ConsumerPoolDiagnostics PoolDiagnostics { get; set; } = new();
        public List<SubscriptionInfo> ActiveSubscriptions { get; set; } = new();
        public Dictionary<Type, ConsumerEntityStats> EntityStatistics { get; set; } = new();
        public Dictionary<string, object> SystemMetrics { get; set; } = new();
    }
}
