using Confluent.Kafka;
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Messaging.Producers.Core;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Abstractions
{

    /// <summary>
    /// 型安全Consumer インターフェース
    /// 設計理由：型安全性確保、購読パターンの統一
    /// 既存Avro実装との統合により高性能なデシリアライゼーション実現
    /// </summary>
    public interface IKafkaConsumer<TValue, TKey> : IDisposable
        where TValue : class
        where TKey : notnull
    {
        /// <summary>
        /// 非同期メッセージストリーム消費
        /// </summary>
        IAsyncEnumerable<KafkaMessage<TValue, TKey>> ConsumeAsync(CancellationToken cancellationToken = default);
        Task<KafkaBatch<TValue, TKey>> ConsumeBatchAsync(KafkaBatchOptions options, CancellationToken cancellationToken = default);
        /// <summary>
        /// オフセットコミット
        /// </summary>
        Task CommitAsync();

        /// <summary>
        /// オフセットシーク
        /// </summary>
        Task SeekAsync(TopicPartitionOffset offset);


        /// <summary>
        /// 割り当てパーティション取得
        /// </summary>
        List<TopicPartition> GetAssignedPartitions();

        /// <summary>
        /// トピック名取得
        /// </summary>
        string TopicName { get; }
    }

}
