using System;

namespace KsqlDsl.Serialization.Avro.Performance
{
    public class PerformanceMetrics
    {
        public long OperationCount { get; set; }
        public long SuccessCount { get; set; }
        public long FailureCount { get; set; }
        public TimeSpan TotalDuration { get; set; }
        public TimeSpan AverageDuration { get; set; }
        public TimeSpan MinDuration { get; set; } = TimeSpan.MaxValue;
        public TimeSpan MaxDuration { get; set; }
        public DateTime LastOperation { get; set; }

        public double SuccessRate => OperationCount > 0 ? (double)SuccessCount / OperationCount : 0.0;
    }

}
