using Confluent.Kafka;
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Producers.Core;
using KsqlDsl.Monitoring.Abstractions.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers.Core
{
    /// <summary>
    /// 型安全Consumer実装（既存Avroデシリアライザー統合版）
    /// 設計理由：既存のEnhancedAvroSerializerManagerを活用し、
    /// 購読状態管理と型安全なデシリアライゼーションを実現
    /// </summary>
    internal class TypedKafkaConsumer<T> : IKafkaConsumer<T> where T : class
    {
        private readonly IConsumer<object, object> _consumer;
        private readonly ConsumerInstance _consumerInstance;
        private readonly IDeserializer<object> _keyDeserializer;
        private readonly IDeserializer<object> _valueDeserializer;
        private readonly string _topicName;
        private readonly EntityModel _entityModel;
        private readonly KafkaSubscriptionOptions _options;
        private readonly ILogger _logger;
        private readonly KafkaConsumerStats _stats = new();
        private bool _subscribed = false;
        private bool _disposed = false;

        public string TopicName => _topicName;

        public TypedKafkaConsumer(
            IConsumer<object, object> consumer,
            ConsumerInstance consumerInstance,
            IDeserializer<object> keyDeserializer,
            IDeserializer<object> valueDeserializer,
            string topicName,
            EntityModel entityModel,
            KafkaSubscriptionOptions options,
            ILogger logger)
        {
            _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            _consumerInstance = consumerInstance ?? throw new ArgumentNullException(nameof(consumerInstance));
            _keyDeserializer = keyDeserializer ?? throw new ArgumentNullException(nameof(keyDeserializer));
            _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
            _topicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            EnsureSubscribed();
        }

        public async IAsyncEnumerable<KafkaMessage<T>> ConsumeAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                KafkaMessage<T>? kafkaMessage = null;

                try
                {
                    kafkaMessage = await ConsumeMessageAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
                catch (Exception ex) when (!IsTerminalException(ex))
                {
                    _logger?.LogWarning(ex, "Non-terminal error consuming message for {EntityType}", typeof(T).Name);
                    await Task.Delay(100, cancellationToken);
                    continue;
                }

                if (kafkaMessage != null)
                {
                    yield return kafkaMessage; // KafkaMessage<T>を返す
                }

                await Task.Delay(10, cancellationToken);
            }
        }
        private async Task<KafkaMessage<T>?> ConsumeMessageAsync(CancellationToken cancellationToken = default)
        {
            return await Task.Run(() =>
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);

                    if (consumeResult != null && !consumeResult.IsPartitionEOF)
                    {
                        return CreateKafkaMessage(consumeResult);
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error consuming message from topic {TopicName}", _topicName);
                    throw;
                }

                return null;
            }, cancellationToken);
        }
        private KafkaMessage<T> CreateKafkaMessage(ConsumeResult<object, object> consumeResult)
        {
            // Value のデシリアライゼーション
            var valueBytes = consumeResult.Message.Value as byte[];
            var message = _valueDeserializer.Deserialize(
                valueBytes ?? Array.Empty<byte>(),
                valueBytes == null,
                new SerializationContext(MessageComponentType.Value, _topicName)) as T;

            if (message == null)
                throw new InvalidOperationException($"Failed to deserialize message to type {typeof(T).Name}");

            // Key のデシリアライゼーション
            var keyBytes = consumeResult.Message.Key as byte[];
            var key = _keyDeserializer.Deserialize(
                keyBytes ?? Array.Empty<byte>(),
                keyBytes == null,
                new SerializationContext(MessageComponentType.Key, _topicName));

            return new KafkaMessage<T>
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

        // 内部実装を分離
        private async IAsyncEnumerable<T> ConsumeInternalAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                T? message = null;

                try
                {
                    // メッセージ消費処理
                    var kafkaMessage = await ConsumeMessageAsync(cancellationToken);
                    if (kafkaMessage != null)
                    {
                        message = kafkaMessage.Value;
                    }
                }
                catch (OperationCanceledException)
                {
                    // キャンセレーション時は正常終了
                    yield break;
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Error consuming message for {EntityType}", typeof(T).Name);

                    // 重大なエラーかどうかを判定
                    if (IsTerminalException(ex))
                    {
                        throw;
                    }

                    // 軽微なエラーの場合は継続
                    await Task.Delay(100, cancellationToken);
                    continue;
                }

                if (message != null)
                {
                    yield return message;
                }

                // 短時間待機（CPU使用率を抑制）
                await Task.Delay(10, cancellationToken);
            }
        }
        private bool IsTerminalException(Exception ex)
        {
            return ex is ArgumentException
                || ex is InvalidOperationException
                || ex is UnauthorizedAccessException
                || ex is OutOfMemoryException
                || ex is ObjectDisposedException;
        }
        public async Task<KafkaBatch<T>> ConsumeBatchAsync(KafkaBatchOptions options, CancellationToken cancellationToken = default)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            var batch = new KafkaBatch<T>
            {
                BatchStartTime = DateTime.UtcNow
            };

            var messages = new List<KafkaMessage<T>>();
            var batchStopwatch = Stopwatch.StartNew();

            try
            {
                EnsureSubscribed();

                var endTime = DateTime.UtcNow.Add(options.MaxWaitTime);
                var rawConsumer = _consumerInstance.PooledConsumer.Consumer;

                while (messages.Count < options.MaxBatchSize &&
                       DateTime.UtcNow < endTime &&
                       !cancellationToken.IsCancellationRequested)
                {
                    var remainingTime = endTime - DateTime.UtcNow;
                    if (remainingTime <= TimeSpan.Zero) break;

                    var consumeResult = rawConsumer.Consume(remainingTime);

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
                        var message = await DeserializeMessageAsync(consumeResult);
                        messages.Add(message);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to deserialize message in batch: {EntityType}",
                            typeof(T).Name);
                    }
                }

                batchStopwatch.Stop();
                batch.BatchEndTime = DateTime.UtcNow;
                batch.Messages = messages;

                UpdateBatchConsumeStats(messages.Count, batchStopwatch.Elapsed);

                return batch;
            }
            catch (Exception ex)
            {
                batchStopwatch.Stop();
                batch.BatchEndTime = DateTime.UtcNow;

                _logger.LogError(ex, "Failed to consume batch: {EntityType} -> {Topic}",
                    typeof(T).Name, _topicName);
                throw;
            }
        }

        public async Task CommitAsync()
        {
            try
            {
                var rawConsumer = _consumerInstance.PooledConsumer.Consumer;
                rawConsumer.Commit();
                await Task.Delay(1);

                _logger.LogTrace("Offset committed: {EntityType} -> {Topic}", typeof(T).Name, _topicName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to commit offset: {EntityType} -> {Topic}",
                    typeof(T).Name, _topicName);
                throw;
            }
        }

        public async Task SeekAsync(TopicPartitionOffset offset)
        {
            if (offset == null)
                throw new ArgumentNullException(nameof(offset));

            try
            {
                var rawConsumer = _consumerInstance.PooledConsumer.Consumer;
                rawConsumer.Seek(offset);
                await Task.Delay(1);

                _logger.LogInformation("Seeked to offset: {EntityType} -> {TopicPartitionOffset}",
                    typeof(T).Name, offset);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to seek to offset: {EntityType} -> {TopicPartitionOffset}",
                    typeof(T).Name, offset);
                throw;
            }
        }

        public KafkaConsumerStats GetStats()
        {
            lock (_stats)
            {
                return new KafkaConsumerStats
                {
                    TotalMessagesReceived = _stats.TotalMessagesReceived,
                    ProcessedMessages = _stats.ProcessedMessages,
                    FailedMessages = _stats.FailedMessages,
                    AverageProcessingTime = _stats.AverageProcessingTime,
                    MinProcessingTime = _stats.MinProcessingTime,
                    MaxProcessingTime = _stats.MaxProcessingTime,
                    LastMessageReceived = _stats.LastMessageReceived,
                    TotalBytesReceived = _stats.TotalBytesReceived,
                    MessagesPerSecond = _stats.MessagesPerSecond,
                    ConsumerLag = new Dictionary<TopicPartition, long>(_stats.ConsumerLag),
                    AssignedPartitions = new List<TopicPartition>(_stats.AssignedPartitions)
                };
            }
        }

        public List<TopicPartition> GetAssignedPartitions()
        {
            try
            {
                var rawConsumer = _consumerInstance.PooledConsumer.Consumer;
                var assignment = rawConsumer.Assignment;
                return assignment?.ToList() ?? new List<TopicPartition>();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get assigned partitions: {EntityType}", typeof(T).Name);
                return new List<TopicPartition>();
            }
        }

        private void EnsureSubscribed()
        {
            if (!_subscribed)
            {
                try
                {
                    var rawConsumer = _consumerInstance.PooledConsumer.Consumer;
                    rawConsumer.Subscribe(_topicName);
                    _subscribed = true;

                    _logger.LogDebug("Subscribed to topic: {EntityType} -> {Topic}", typeof(T).Name, _topicName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to subscribe to topic: {EntityType} -> {Topic}",
                        typeof(T).Name, _topicName);
                    throw;
                }
            }
        }

        private async Task<KafkaMessage<T>> DeserializeMessageAsync(ConsumeResult<object, object> consumeResult)
        {
            await Task.Delay(1);

            try
            {
                // 既存EnhancedAvroSerializerManagerのデシリアライザーを活用
                T value;
                if (consumeResult.Message.Value != null)
                {
                    // 実際の実装では適切なAvroデータを使用
                    var deserializedValue = _valueDeserializer.Deserialize(
                        ReadOnlySpan<byte>.Empty,
                        false,
                        new SerializationContext(MessageComponentType.Value, _topicName));

                    value = (T)deserializedValue;
                }
                else
                {
                    throw new InvalidOperationException("Message value cannot be null");
                }

                object? key = null;
                if (consumeResult.Message.Key != null)
                {
                    key = _keyDeserializer.Deserialize(
                        ReadOnlySpan<byte>.Empty,
                        false,
                        new SerializationContext(MessageComponentType.Key, _topicName));
                }

                return new KafkaMessage<T>
                {
                    Value = value,
                    Key = key,
                    Topic = consumeResult.Topic,
                    Partition = consumeResult.Partition.Value,
                    Offset = consumeResult.Offset.Value,
                    Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                    Headers = consumeResult.Message.Headers
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to deserialize message: {EntityType} -> {Topic}",
                    typeof(T).Name, _topicName);
                throw;
            }
        }

        private void UpdateConsumeStats(bool success, TimeSpan processingTime)
        {
            lock (_stats)
            {
                _stats.TotalMessagesReceived++;

                if (success)
                    _stats.ProcessedMessages++;
                else
                    _stats.FailedMessages++;

                if (_stats.MinProcessingTime == TimeSpan.Zero || processingTime < _stats.MinProcessingTime)
                    _stats.MinProcessingTime = processingTime;
                if (processingTime > _stats.MaxProcessingTime)
                    _stats.MaxProcessingTime = processingTime;

                if (_stats.TotalMessagesReceived == 1)
                {
                    _stats.AverageProcessingTime = processingTime;
                }
                else
                {
                    var totalMs = _stats.AverageProcessingTime.TotalMilliseconds * (_stats.TotalMessagesReceived - 1) + processingTime.TotalMilliseconds;
                    _stats.AverageProcessingTime = TimeSpan.FromMilliseconds(totalMs / _stats.TotalMessagesReceived);
                }

                _stats.LastMessageReceived = DateTime.UtcNow;
            }
        }

        private void UpdateBatchConsumeStats(int messageCount, TimeSpan processingTime)
        {
            lock (_stats)
            {
                _stats.TotalMessagesReceived += messageCount;
                _stats.ProcessedMessages += messageCount;
                _stats.LastMessageReceived = DateTime.UtcNow;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                try
                {
                    var rawConsumer = _consumerInstance.PooledConsumer.Consumer;

                    if (_subscribed)
                    {
                        rawConsumer.Unsubscribe();
                        _subscribed = false;
                    }

                    // ConsumerInstanceはプールに返却せず、適切に終了
                    // 理由：Consumer状態管理の複雑性により、プール返却は危険
                    rawConsumer.Close();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing consumer: {EntityType}", typeof(T).Name);
                }
                _disposed = true;
            }
        }
    }
}