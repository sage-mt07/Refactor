using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Options
{

    /// <summary>
    /// Consumer健全性閾値
    /// </summary>
    public class ConsumerHealthThresholds
    {
        public long MaxAverageProcessingTimeMs { get; set; } = 500;
        public long CriticalProcessingTimeMs { get; set; } = 5000;
        public double MaxFailureRate { get; set; } = 0.1;
        public double CriticalFailureRate { get; set; } = 0.2;
        public long MaxConsumerLag { get; set; } = 10000;
        public long CriticalConsumerLag { get; set; } = 100000;
    }
}
