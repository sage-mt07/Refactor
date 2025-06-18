using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models
{

    /// <summary>
    /// Producer全体パフォーマンス統計
    /// </summary>
    public class ProducerPerformanceStats
    {
        public long TotalMessages { get; set; }
        public long TotalBatches { get; set; }
        public long SuccessfulMessages { get; set; }
        public long FailedMessages { get; set; }
        public double FailureRate => TotalMessages > 0 ? (double)FailedMessages / TotalMessages : 0;
        public TimeSpan AverageLatency { get; set; }
        public double ThroughputPerSecond { get; set; }
        public int ActiveProducers { get; set; }
        public Dictionary<Type, ProducerEntityStats> EntityStats { get; set; } = new();
        public DateTime LastUpdated { get; set; }

        // 内部統計フィールド
        public long TotalProducersCreated;
        public long ProducerCreationFailures;
        public DateTime LastThroughputCalculation;
    }

}
