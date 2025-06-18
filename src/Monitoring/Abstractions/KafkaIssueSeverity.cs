using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{

    /// <summary>
    /// 問題深刻度
    /// </summary>
    public enum KafkaIssueSeverity
    {
        Low,
        Medium,
        High,
        Critical
    }

}
