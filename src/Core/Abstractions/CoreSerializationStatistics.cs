using System;

namespace KsqlDsl.Core.Abstractions
{

    public class CoreSerializationStatistics
    {
        public long TotalOperations { get; set; }
        public long SuccessfulOperations { get; set; }
        public long FailedOperations { get; set; }
        public TimeSpan AverageLatency { get; set; }
        public DateTime LastUpdated { get; set; }

        /// <summary>
        /// 成功率（0.0 ～ 1.0）
        /// </summary>
        public double SuccessRate => TotalOperations > 0 ? (double)SuccessfulOperations / TotalOperations : 0.0;

        /// <summary>
        /// 失敗率（0.0 ～ 1.0）
        /// </summary>
        public double FailureRate => TotalOperations > 0 ? (double)FailedOperations / TotalOperations : 0.0;

        /// <summary>
        /// ヒット率（成功率の別名）
        /// </summary>
        public double HitRate => SuccessRate;

        /// <summary>
        /// ミス率（失敗率の別名）
        /// </summary>
        public double MissRate => FailureRate;

        /// <summary>
        /// 統計情報のリセット
        /// </summary>
        public void Reset()
        {
            TotalOperations = 0;
            SuccessfulOperations = 0;
            FailedOperations = 0;
            AverageLatency = TimeSpan.Zero;
            LastUpdated = DateTime.UtcNow;
        }

        /// <summary>
        /// 操作の記録
        /// </summary>
        public void RecordOperation(bool success, TimeSpan latency)
        {
            TotalOperations++;

            if (success)
                SuccessfulOperations++;
            else
                FailedOperations++;

            // 移動平均でレイテンシを更新
            if (TotalOperations == 1)
            {
                AverageLatency = latency;
            }
            else
            {
                var totalTicks = (AverageLatency.Ticks * (TotalOperations - 1)) + latency.Ticks;
                AverageLatency = new TimeSpan(totalTicks / TotalOperations);
            }

            LastUpdated = DateTime.UtcNow;
        }

        /// <summary>
        /// 統計情報の文字列表現
        /// </summary>
        public override string ToString()
        {
            return $"Operations: {TotalOperations}, Success: {SuccessfulOperations} ({SuccessRate:P2}), " +
                   $"Failed: {FailedOperations} ({FailureRate:P2}), Avg Latency: {AverageLatency.TotalMilliseconds:F2}ms";
        }
    }
}
