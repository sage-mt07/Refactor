using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
{

    /// <summary>
    /// Consumer全体パフォーマンス統計
    /// </summary>
    public class ConsumerPerformanceStats
    {
        public long TotalMessages;
        public long TotalBatches;
        public long ProcessedMessages;
        public long FailedMessages;
        public double FailureRate => TotalMessages > 0 ? (double)FailedMessages / TotalMessages : 0;
        public TimeSpan AverageProcessingTime { get; set; }
        public double ThroughputPerSecond { get; set; }
        public int ActiveConsumers { get; set; }
        public int ActiveSubscriptions { get; set; }
        public Dictionary<Type, ConsumerEntityStats> EntityStats { get; set; } = new();
        public DateTime LastUpdated { get; set; }

        // 内部統計フィールド
        public long TotalConsumersCreated;
        public long ConsumerCreationFailures;
        public DateTime LastThroughputCalculation;
    }
}
