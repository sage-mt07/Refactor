using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Producer健全性レベル
    /// </summary>
    public enum ProducerHealthLevel
    {
        Healthy,
        Warning,
        Critical
    }
}
