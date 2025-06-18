using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// プール問題深刻度
    /// </summary>
    public enum PoolIssueSeverity
    {
        Low,
        Medium,
        High,
        Critical,
        Warning
    }
}
