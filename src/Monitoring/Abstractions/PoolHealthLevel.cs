using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// プール健全性レベル
    /// </summary>
    public enum PoolHealthLevel
    {
        Healthy,
        Warning,
        Critical
    }
}
