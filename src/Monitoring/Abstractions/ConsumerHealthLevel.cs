using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Consumer健全性レベル
    /// </summary>
    public enum ConsumerHealthLevel
    {
        Healthy,
        Warning,
        Critical
    }
}
