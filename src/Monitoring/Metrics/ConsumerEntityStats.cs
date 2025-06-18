using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Metrics
{

    /// <summary>
    /// Consumer エンティティ別統計
    /// </summary>
    public class ConsumerEntityStats
    {
        public Type EntityType { get; set; } = default!;
        public long TotalMessages { get; set; }
        public long TotalBatches { get; set; }
        public long ProcessedMessages { get; set; }
        public long FailedMessages { get; set; }
        public TimeSpan TotalProcessingTime { get; set; }
        public TimeSpan AverageProcessingTime { get; set; }
        public long ConsumersCreated { get; set; }
        public long CreationFailures { get; set; }
        public TimeSpan TotalCreationTime { get; set; }
        public TimeSpan AverageCreationTime { get; set; }
        public DateTime LastActivity { get; set; }
        public DateTime LastFailure { get; set; }
        public string? LastFailureReason { get; set; }
    }
}
