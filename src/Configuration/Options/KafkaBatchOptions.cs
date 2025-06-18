using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Options
{
    /// <summary>
    /// バッチオプション
    /// </summary>
    public class KafkaBatchOptions
    {
        public int MaxBatchSize { get; set; } = 100;
        public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);
        public bool AutoCommit { get; set; } = true;
        public bool EnableEmptyBatches { get; set; } = false;
        public string? ConsumerGroupId { get; set; }
    }


}
