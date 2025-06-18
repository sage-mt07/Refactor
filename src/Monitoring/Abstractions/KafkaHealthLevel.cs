using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Kafka健全性レベル
    /// </summary>
    public enum KafkaHealthLevel
    {
        Healthy,
        Warning,
        Critical
    }
}
