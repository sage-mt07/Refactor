
// =============================================================================
// Phase2変更完了：型安全Consumer実装（統合版）
// 変更理由：重複していた旧KafkaConsumer<T>を削除し、TypedKafkaConsumer<T>に統一
// =============================================================================

using Confluent.Kafka;
using KsqlDsl.Communication;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Phase2完了版：型安全Consumer実装（統合版）
/// 変更理由：重複していた旧実装を削除し、TypedKafkaConsumerへの統一ラッパー
/// </summary>
public class KafkaConsumer<T> : IKafkaConsumer<T> where T : class
{
    private readonly TypedKafkaConsumer<T> _typedConsumer;
    private readonly KafkaConsumerManager _manager;
    private bool _disposed = false;

    public string TopicName => _typedConsumer.TopicName;

    /// <summary>
    /// Phase2変更：TypedKafkaConsumerをラップする形に変更
    /// </summary>
    public KafkaConsumer(
        TypedKafkaConsumer<T> typedConsumer,
        KafkaConsumerManager manager)
    {
        _typedConsumer = typedConsumer ?? throw new ArgumentNullException(nameof(typedConsumer));
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
    }

    /// <summary>
    /// Phase2変更：内部でTypedKafkaConsumerを使用
    /// </summary>
    public async IAsyncEnumerable<KafkaMessage<T>> ConsumeAsync(CancellationToken cancellationToken = default)
    {
        await foreach (var message in _typedConsumer.ConsumeAsync(cancellationToken))
        {
            yield return message;
        }
    }

    /// <summary>
    /// Phase2変更：内部でTypedKafkaConsumerを使用
    /// </summary>
    public async Task<KafkaBatch<T>> ConsumeBatchAsync(KafkaBatchOptions options, CancellationToken cancellationToken = default)
    {
        return await _typedConsumer.ConsumeBatchAsync(options, cancellationToken);
    }

    /// <summary>
    /// Phase2変更：TypedKafkaConsumerのCommit機能使用
    /// </summary>
    public async Task CommitAsync()
    {
        await _typedConsumer.CommitAsync();
    }

    /// <summary>
    /// Phase2変更：TypedKafkaConsumerのSeek機能使用
    /// </summary>
    public async Task SeekAsync(TopicPartitionOffset offset)
    {
        await _typedConsumer.SeekAsync(offset);
    }

    /// <summary>
    /// Phase2変更：TypedKafkaConsumerから統計取得
    /// </summary>
    public KafkaConsumerStats GetStats()
    {
        return _typedConsumer.GetStats();
    }

    /// <summary>
    /// Phase2変更：TypedKafkaConsumerのパーティション情報取得
    /// </summary>
    public List<TopicPartition> GetAssignedPartitions()
    {
        return _typedConsumer.GetAssignedPartitions();
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                // Phase2変更：TypedConsumerの適切な破棄
                _typedConsumer.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARNING] Phase2: Error disposing consumer wrapper: {ex.Message}");
            }

            _disposed = true;
        }
    }
}
