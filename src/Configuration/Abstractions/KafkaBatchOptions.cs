using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Abstractions
{
    public class KafkaBatchOptions
    {
        /// <summary>
        /// バッチ最大サイズ
        /// </summary>
        public int MaxBatchSize { get; set; } = 100;

        /// <summary>
        /// 最大待機時間
        /// </summary>
        public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// 空バッチを許可するか
        /// </summary>
        public bool EnableEmptyBatches { get; set; } = false;

        /// <summary>
        /// 自動コミット有効化
        /// </summary>
        public bool AutoCommit { get; set; } = true;

        /// <summary>
        /// コンシューマーグループID
        /// </summary>
        public string? ConsumerGroupId { get; set; }
    }
}
