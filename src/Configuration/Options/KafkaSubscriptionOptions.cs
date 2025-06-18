using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Options
{

    // =============================================================================
    // Configuration Classes - 設定・オプションクラス
    // =============================================================================

    /// <summary>
    /// 購読オプション
    /// </summary>
    public class KafkaSubscriptionOptions
    {
        public string? GroupId { get; set; }
        public bool AutoCommit { get; set; } = true;
        public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;
        public bool EnablePartitionEof { get; set; } = false;
        public TimeSpan SessionTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(3);
        public bool StopOnError { get; set; } = false;
        public int MaxPollRecords { get; set; } = 500;
        public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromMinutes(5);
    }


}
