using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{



    /// <summary>
    /// プール健全性問題タイプ
    /// </summary>
    public enum PoolHealthIssueType
    {
        HighFailureRate,
        PoolExhaustion,
        ResourceLeak,
        HealthCheckFailure
    }
}
