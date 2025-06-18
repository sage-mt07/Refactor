using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Consumer健全性問題タイプ
    /// </summary>
    public enum ConsumerHealthIssueType
    {
        HighFailureRate,
        SlowProcessing,
        HighConsumerLag,
        RebalanceIssue,
        PoolExhaustion,
        HealthCheckFailure
    }
}
