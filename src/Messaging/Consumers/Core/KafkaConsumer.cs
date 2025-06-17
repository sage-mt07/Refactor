using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KsqlDsl.Communication;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Modeling;
using Microsoft.Extensions.Logging;

namespace KsqlDsl.Messaging.Consumers.Core
{
    public class KafkaConsumer<T> : IKafkaConsumer<T> where T : class
    {
        private readonly IConsumer<object, object> _rawConsumer;
        private readonly IDeserializer<object> _keyDeserializer;
        private readonly IDeserializer<object> _valueDeserializer;
        private readonly EntityModel _entityModel;
        private readonly SubscriptionOptions _options;
        private readonly IConsumerMetricsCollector _metricsCollector;
        private readonly ILogger _logger;
        private readonly KafkaConsumerStats _stats = new();
        private bool _subscribed = false;
        private bool _disposed = false;

        public string TopicName { get; }

        public KafkaConsumer(
            IConsumer<object, object> rawConsumer,
            IDeserializer<object> keyDeserializer,
            IDeserializer<object> valueDeserializer,
            string topicName,
            EntityModel entityModel,
            SubscriptionOptions options,
            IConsumerMetricsCollector metricsCollector,
            ILogger logger)
        {
            _rawConsumer = rawConsumer ?? throw new ArgumentNullException(nameof(rawConsumer));
            _keyDeserializer = keyDeserializer ?? throw new ArgumentNullException(nameof(keyDeserializer));
            _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
            TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _metricsCollector = metricsCollector ?? throw new ArgumentNullException(nameof(metricsCollector));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            EnsureSubscribed();
        }

        public async IAsyncEnumerable<KafkaMessage<T>> ConsumeAsync(CancellationToken cancellationToken = default)
        {
            EnsureSubscribed();

            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<object, object>? consumeResult = null;
                var stopwatch = Stopwatch.StartNew();

                try
                {
                    consumeResult = _rawConsumer.Consume(TimeSpan.FromSeconds(1));

                    if (consumeResult == null)
                        continue;

                    if (consumeResult.IsPartitionEOF)
                        continue;

                    stopwatch.Stop();

                    var message = await DeserializeMessageAsync(consumeResult);
                    _metricsCollector.RecordConsume(success: true, stopwatch.Elapsed, consumeResult.Message.Value?.ToString()?.Length ?? 0);
                    UpdateConsumeStats(success: true, stopwatch.Elapsed);

                    yield return message;
                }
                catch (ConsumeException ex)
                {
                    stopwatch.Stop();
                    _metricsCollector.RecordConsume(success: false, stopwatch.Elapsed, 0);
                    UpdateConsumeStats(success: false, stopwatch.Elapsed);

                    if (ex.Error.IsFatal)
                        throw;
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
                catch (Exception ex)
                {
                    stopwatch.Stop();
                    _metricsCollector.RecordConsume(success: false, stopwatch.Elapsed, 0);
                    UpdateConsumeStats(success: false, stopwatch.Elapsed);
                    _logger.LogError(ex, "Unexpected consume error: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
                    throw;
                }
            }
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

                while (messages.Count < options.MaxBatchSize &&
                       DateTime.UtcNow < endTime &&
                       !cancellationToken.IsCancellationRequested)
                {
                    var remainingTime = endTime - DateTime.UtcNow;
                    if (remainingTime <= TimeSpan.Zero) break;

                    var consumeResult = _rawConsumer.Consume(remainingTime);

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
                        _logger.LogWarning(ex, "Failed to deserialize message in batch: {EntityType}", typeof(T).Name);
                    }
                }

                batchStopwatch.Stop();
                batch.BatchEndTime = DateTime.UtcNow;
                batch.Messages = messages;

                _metricsCollector.RecordBatch(messages.Count, batchStopwatch.Elapsed);
                UpdateBatchConsumeStats(messages.Count, batchStopwatch.Elapsed);

                return batch;
            }
            catch (Exception ex)
            {
                batchStopwatch.Stop();
                batch.BatchEndTime = DateTime.UtcNow;
                _logger.LogError(ex, "Failed to consume batch: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
                throw;
            }
        }

        public async Task CommitAsync()
        {
            try
            {
                _rawConsumer.Commit();
                await Task.Delay(1);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to commit offset: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
                throw;
            }
        }

        public async Task SeekAsync(TopicPartitionOffset offset)
        {
            if (offset == null)
                throw new ArgumentNullException(nameof(offset));

            try
            {
                _rawConsumer.Seek(offset);
                await Task.Delay(1);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to seek to offset: {EntityType} -> {TopicPartitionOffset}", typeof(T).Name, offset);
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
                var assignment = _rawConsumer.Assignment;
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
                    _rawConsumer.Subscribe(TopicName);
                    _subscribed = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to subscribe to topic: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
                    throw;
                }
            }
        }

        private async Task<KafkaMessage<T>> DeserializeMessageAsync(ConsumeResult<object, object> consumeResult)
        {
            await Task.Delay(1);

            try
            {
                T value;
                if (consumeResult.Message.Value != null)
                {
                    var deserializedValue = _valueDeserializer.Deserialize(
                        ReadOnlySpan<byte>.Empty,
                        false,
                        new SerializationContext(MessageComponentType.Value, TopicName));

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
                        new SerializationContext(MessageComponentType.Key, TopicName));
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
                _logger.LogError(ex, "Failed to deserialize message: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
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
                    if (_subscribed)
                    {
                        _rawConsumer.Unsubscribe();
                        _subscribed = false;
                    }

                    _rawConsumer.Close();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing consumer: {EntityType}", typeof(T).Name);
                }

                _disposed = true;
            }
        }
    }

    public interface IConsumerMetricsCollector
    {
        void RecordConsume(bool success, TimeSpan duration, int messageSize);
        void RecordBatch(int messageCount, TimeSpan duration);
    }
}