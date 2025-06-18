using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{

    public enum CacheIssueType
    {
        LowHitRate,
        ExcessiveMisses,
        StaleSchemas,
        MemoryPressure,
        SchemaVersionMismatch
    }
}
