using Confluent.Kafka;
using System;
namespace KsqlDsl.Configuration.Abstractions
{
    public class KafkaSubscriptionOptions
    {
        /// <summary>
        /// コンシューマーグループID
        /// </summary>
        public string? GroupId { get; set; }

        /// <summary>
        /// 自動コミット有効化
        /// </summary>
        public bool? AutoCommit { get; set; }

        /// <summary>
        /// オートオフセットリセット（Confluent.Kafka使用）
        /// </summary>
        public AutoOffsetReset? AutoOffsetReset { get; set; }

        /// <summary>
        /// パーティションEOF有効化
        /// </summary>
        public bool EnablePartitionEof { get; set; } = false;

        /// <summary>
        /// セッションタイムアウト
        /// </summary>
        public TimeSpan? SessionTimeout { get; set; }

        /// <summary>
        /// ハートビート間隔
        /// </summary>
        public TimeSpan? HeartbeatInterval { get; set; }

        /// <summary>
        /// エラー時停止
        /// </summary>
        public bool StopOnError { get; set; } = false;

        /// <summary>
        /// 最大ポーリングレコード数
        /// </summary>
        public int? MaxPollRecords { get; set; }

        /// <summary>
        /// 最大ポーリング間隔
        /// </summary>
        public TimeSpan? MaxPollInterval { get; set; }
    }
}
