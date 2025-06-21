using KsqlDsl.Core.Abstractions;
using KsqlDsl.Messaging.Producers.Core;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Abstractions
{

    // =============================================================================
    // Core Interfaces - 型安全な Producer/Consumer の定義
    // =============================================================================

    /// <summary>
    /// 型安全Producer インターフェース
    /// 設計理由：型安全性確保、テスタビリティ向上
    /// 既存Avro実装との統合により高性能なシリアライゼーション実現
    /// </summary>
    public interface IKafkaProducer<T> : IDisposable where T : class
    {
        /// <summary>
        /// 単一メッセージ送信
        /// </summary>
        Task<KafkaDeliveryResult> SendAsync(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// バッチメッセージ送信
        /// </summary>
        Task<KafkaBatchDeliveryResult> SendBatchAsync(IEnumerable<T> messages, KafkaMessageContext? context = null, CancellationToken cancellationToken = default);

 

        /// <summary>
        /// 保留中メッセージのフラッシュ
        /// </summary>
        Task FlushAsync(TimeSpan timeout);

        /// <summary>
        /// トピック名取得
        /// </summary>
        string TopicName { get; }
    }


}
