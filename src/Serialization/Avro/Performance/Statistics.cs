using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Performance
{
    internal class Statistics
    {
        public Dictionary<string, long> Counters { get; set; } = new();
        public Dictionary<string, TimeSpan> Durations { get; set; } = new();
        public DateTime LastUpdated { get; set; }
    }
}
