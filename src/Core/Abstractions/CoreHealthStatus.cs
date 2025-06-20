using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions
{
    public enum CoreHealthStatus
    {
        Healthy,
        Degraded,
        Unhealthy,
        Unknown
    }
}
