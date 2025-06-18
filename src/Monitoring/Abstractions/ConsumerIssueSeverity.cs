using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{


    /// <summary>
    /// Consumer問題深刻度
    /// </summary>
    public enum ConsumerIssueSeverity
    {
        Low,
        Medium,
        High,
        Critical,
        Warning
    }
}
