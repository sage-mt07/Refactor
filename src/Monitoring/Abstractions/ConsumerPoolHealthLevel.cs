using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{

    /// <summary>
    /// Consumerプール健全性レベル
    /// </summary>
    public enum ConsumerPoolHealthLevel
    {
        Healthy,
        Warning,
        Critical
    }
}
