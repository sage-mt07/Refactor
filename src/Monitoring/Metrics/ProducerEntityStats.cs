using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Metrics
{

    /// <summary>
    /// Producer エンティティ別統計
    /// </summary>
    public class ProducerEntityStats
    {
        public Type EntityType { get; set; } = default!;
        public long TotalMessages { get; set; }
        public long TotalBatches { get; set; }
        public long SuccessfulMessages { get; set; }
        public long FailedMessages { get; set; }
        public long SuccessfulBatches { get; set; }
        public long FailedBatches { get; set; }
        public TimeSpan TotalSendTime { get; set; }
        public TimeSpan AverageSendTime { get; set; }
        public long ProducersCreated { get; set; }
        public long CreationFailures { get; set; }
        public TimeSpan TotalCreationTime { get; set; }
        public TimeSpan AverageCreationTime { get; set; }
        public DateTime LastActivity { get; set; }
        public DateTime LastFailure { get; set; }
        public string? LastFailureReason { get; set; }
    }
}
