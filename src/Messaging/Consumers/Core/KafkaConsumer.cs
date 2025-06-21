using Confluent.Kafka;
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Producers.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers.Core
{
    /// <summary>
    /// 統合型安全Consumer - TypedKafkaConsumer + KafkaConsumer統合版
    /// 設計理由: Pool削除、Confluent.Kafka完全委譲、シンプル化
    /// </summary>
    public class KafkaConsumer<TValue, TKey> : IKafkaConsumer<TValue, TKey>
        where TValue : class
        where TKey : notnull
    {
        private readonly IConsumer<object, object> _consumer;
        private readonly IDeserializer<object> _keyDeserializer;
        private readonly IDeserializer<object> _valueDeserializer;
        private readonly EntityModel _entityModel;
        private readonly ILogger? _logger;
        private bool _subscribed = false;
        private bool _disposed = false;

        public string TopicName { get; }

        public KafkaConsumer(
            IConsumer<object, object> consumer,
            IDeserializer<object> keyDeserializer,
            IDeserializer<object> valueDeserializer,
            string topicName,
            EntityModel entityModel,
            ILoggerFactory? loggerFactory = null)
        {
            _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            _keyDeserializer = keyDeserializer ?? throw new ArgumentNullException(nameof(keyDeserializer));
            _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
            TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _logger = loggerFactory.CreateLoggerOrNull<KafkaConsumer<TValue, TKey>>();

            EnsureSubscribed();
        }

        public async IAsyncEnumerable<KafkaMessage<TValue, TKey>> ConsumeAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                KafkaMessage<TValue, TKey>? kafkaMessage = null;

                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);

                    if (consumeResult != null && !consumeResult.IsPartitionEOF)
                    {
                        kafkaMessage = CreateKafkaMessage(consumeResult);
                    }
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Error consuming message from topic {TopicName}", TopicName);
                    await Task.Delay(100, cancellationToken);
                    continue;
                }

                if (kafkaMessage != null)
                {
                    yield return kafkaMessage;
                }

                await Task.Delay(10, cancellationToken);
            }
        }

        public Task<KafkaBatch<TValue, TKey>> ConsumeBatchAsync(KafkaBatchOptions options, CancellationToken cancellationToken = default)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            var batch = new KafkaBatch<TValue, TKey>
            {
                BatchStartTime = DateTime.UtcNow
            };

            var messages = new List<KafkaMessage<TValue, TKey>>();
            var endTime = DateTime.UtcNow.Add(options.MaxWaitTime);

            try
            {
                EnsureSubscribed();

                while (messages.Count < options.MaxBatchSize &&
                       DateTime.UtcNow < endTime &&
                       !cancellationToken.IsCancellationRequested)
                {
                    var remainingTime = endTime - DateTime.UtcNow;
                    if (remainingTime <= TimeSpan.Zero) break;

                    var consumeResult = _consumer.Consume(remainingTime);

                    if (consumeResult == null)
                        break;

                    if (consumeResult.IsPartitionEOF)
                    {
                        if (options.EnableEmptyBatches)
                            break;
                        continue;
                    }

                    try
                    {
                        var message = CreateKafkaMessage(consumeResult);
                        messages.Add(message);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning(ex, "Failed to deserialize message in batch: {EntityType}", typeof(TValue).Name);
                    }
                }

                batch.BatchEndTime = DateTime.UtcNow;
                batch.Messages = messages;

                return Task.FromResult(batch);
            }
            catch (Exception ex)
            {
                batch.BatchEndTime = DateTime.UtcNow;
                _logger?.LogError(ex, "Failed to consume batch: {EntityType} -> {Topic}", typeof(TValue).Name, TopicName);
                throw;
            }
        }
        public async Task CommitAsync()
        {
            try
            {
                _consumer.Commit();
                await Task.Delay(1);
                _logger?.LogTrace("Offset committed: {EntityType} -> {Topic}", typeof(TValue).Name, TopicName);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to commit offset: {EntityType} -> {Topic}", typeof(TValue).Name, TopicName);
                throw;
            }
        }

        public async Task SeekAsync(TopicPartitionOffset offset)
        {
            if (offset == null)
                throw new ArgumentNullException(nameof(offset));

            try
            {
                _consumer.Seek(offset);
                await Task.Delay(1);
                _logger?.LogInformation("Seeked to offset: {EntityType} -> {TopicPartitionOffset}", typeof(TValue).Name, offset);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to seek to offset: {EntityType} -> {TopicPartitionOffset}", typeof(   TValue).Name, offset);
                throw;
            }
        }



        public List<TopicPartition> GetAssignedPartitions()
        {
            try
            {
                var assignment = _consumer.Assignment;
                return assignment?.ToList() ?? new List<TopicPartition>();
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to get assigned partitions: {EntityType}", typeof(TValue).Name);
                return new List<TopicPartition>();
            }
        }

        private void EnsureSubscribed()
        {
            if (!_subscribed)
            {
                try
                {
                    _consumer.Subscribe(TopicName);
                    _subscribed = true;
                    _logger?.LogDebug("Subscribed to topic: {EntityType} -> {Topic}", typeof(TValue).Name, TopicName);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed to subscribe to topic: {EntityType} -> {Topic}", typeof(TValue).Name, TopicName);
                    throw;
                }
            }
        }

        private KafkaMessage<TValue, TKey> CreateKafkaMessage(ConsumeResult<object, object> consumeResult)
        {
            var valueBytes = consumeResult.Message.Value as byte[];
            var message = _valueDeserializer.Deserialize(
                valueBytes ?? Array.Empty<byte>(),
                valueBytes == null,
                new SerializationContext(MessageComponentType.Value, TopicName)) as TValue;

            if (message == null)
                throw new InvalidOperationException($"Failed to deserialize message to type {typeof(TValue).Name}");

            var keyBytes = consumeResult.Message.Key as byte[];
            var keyObject = _keyDeserializer.Deserialize(
                keyBytes ?? Array.Empty<byte>(),
                keyBytes == null,
                new SerializationContext(MessageComponentType.Key, TopicName));

            TKey key;
            if (keyObject is TKey typedKey)
            {
                key = typedKey;
            }
            else if (keyObject == null && !typeof(TKey).IsValueType)
            {
                key = default(TKey)!; // 参照型でnullの場合
            }
            else
            {
                // 型変換失敗時の例外
                throw new InvalidOperationException(
                    $"Failed to convert key from {keyObject?.GetType()?.Name ?? "null"} to {typeof(TKey).Name}");
            }
            return new KafkaMessage<TValue, TKey>
            {
                Value = message,
                Key = key,
                Topic = consumeResult.Topic,
                Partition = consumeResult.Partition.Value,
                Offset = consumeResult.Offset.Value,
                Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                Headers = consumeResult.Message.Headers,
                Context = new KafkaMessageContext
                {
                    MessageId = Guid.NewGuid().ToString(),
                    CorrelationId = ExtractCorrelationId(consumeResult.Message.Headers),
                    Tags = new Dictionary<string, object>
                    {
                        ["topic"] = consumeResult.Topic,
                        ["partition"] = consumeResult.Partition.Value,
                        ["offset"] = consumeResult.Offset.Value
                    }
                }
            };
        }

        private string? ExtractCorrelationId(Headers? headers)
        {
            if (headers == null) return null;

            try
            {
                var correlationIdHeader = headers.FirstOrDefault(h => h.Key == "correlationId");
                if (correlationIdHeader != null && correlationIdHeader.GetValueBytes() != null)
                {
                    return System.Text.Encoding.UTF8.GetString(correlationIdHeader.GetValueBytes());
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to extract correlation ID from headers");
            }

            return null;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                try
                {
                    if (_subscribed)
                    {
                        _consumer.Unsubscribe();
                        _subscribed = false;
                    }
                    _consumer.Close();
                    _consumer.Dispose();
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Error disposing consumer: {EntityType}", typeof(TValue).Name);
                }
                _disposed = true;
            }
        }
    }
}