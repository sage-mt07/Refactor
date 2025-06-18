using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Consumerプール健全性問題タイプ
    /// </summary>
    public enum ConsumerPoolHealthIssueType
    {
        HighFailureRate,
        PoolExhaustion,
        RebalanceFailure,
        HealthCheckFailure
    }
}
