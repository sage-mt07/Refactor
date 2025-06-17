using Confluent.Kafka;
using KsqlDsl.Modeling;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Communication;

// =============================================================================
// Phase2変更完了：重複Implementation削除
// 旧KafkaProducer<T>とKafkaConsumer<T>は削除済み
// TypedKafkaProducer<T>とTypedKafkaConsumer<T>に統一
// =============================================================================

/// <summary>
/// Phase2完了版：型安全Producer実装（統合版）
/// 変更理由：重複していた旧KafkaProducer<T>を削除し、TypedKafka*に統一
/// 既存のEnhancedAvroSerializerManagerとの統合により高性能・型安全な通信を実現
/// </summary>
public class KafkaProducer<T> : IKafkaProducer<T> where T : class
{
    private readonly TypedKafkaProducer<T> _typedProducer;
    private readonly KafkaProducerManager _manager;
    private bool _disposed = false;

    public string TopicName => _typedProducer.TopicName;

    /// <summary>
    /// Phase2変更：TypedKafkaProducerをラップする形に変更
    /// </summary>
    public KafkaProducer(
        TypedKafkaProducer<T> typedProducer,
        KafkaProducerManager manager)
    {
        _typedProducer = typedProducer ?? throw new ArgumentNullException(nameof(typedProducer));
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
    }

    /// <summary>
    /// Phase2変更：内部でTypedKafkaProducerを使用
    /// </summary>
    public async Task<KafkaDeliveryResult> SendAsync(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
    {
        return await _typedProducer.SendAsync(message, context, cancellationToken);
    }

    /// <summary>
    /// Phase2変更：内部でTypedKafkaProducerを使用
    /// </summary>
    public async Task<KafkaBatchDeliveryResult> SendBatchAsync(IEnumerable<T> messages, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
    {
        return await _typedProducer.SendBatchAsync(messages, context, cancellationToken);
    }

    /// <summary>
    /// Phase2変更：TypedKafkaProducerから統計取得
    /// </summary>
    public KafkaProducerStats GetStats()
    {
        return _typedProducer.GetStats();
    }

    /// <summary>
    /// Phase2変更：TypedKafkaProducerのFlush機能使用
    /// </summary>
    public async Task FlushAsync(TimeSpan timeout)
    {
        await _typedProducer.FlushAsync(timeout);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                // Phase2変更：ManagerへのProducer返却はTyped版を使用
                _manager.ReturnProducer(this);
                _typedProducer.Dispose();
            }
            catch (Exception ex)
            {
                // ログ出力は内部でTypedKafkaProducerが実行
                Console.WriteLine($"[WARNING] Phase2: Error disposing producer wrapper: {ex.Message}");
            }

            _disposed = true;
        }
    }
}