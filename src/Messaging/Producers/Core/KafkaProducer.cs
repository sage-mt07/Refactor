
// =============================================================================
// src/Messaging/Producers/Core/KafkaProducer.cs
// =============================================================================

using Confluent.Kafka;
using KsqlDsl.Avro;
using KsqlDsl.Communication;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Messaging.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Producers.Core
{
    public class KafkaProducer<T> : IKafkaProducer<T> where T : class
    {
        private readonly IProducer<object, object> _rawProducer;
        private readonly ISerializer<object> _keySerializer;
        private readonly ISerializer<object> _valueSerializer;
        private readonly EntityModel _entityModel;
        private readonly ILogger _logger;
        private readonly KafkaProducerStats _stats = new();
        private bool _disposed = false;

        public string TopicName { get; }

        internal IProducer<object, object> RawProducer => _rawProducer;

        public KafkaProducer(
            IProducer<object, object> rawProducer,
            ISerializer<object> keySerializer,
            ISerializer<object> valueSerializer,
            string topicName,
            EntityModel entityModel,
            ILogger logger)
        {
            _rawProducer = rawProducer ?? throw new ArgumentNullException(nameof(rawProducer));
            _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
            TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<KafkaDeliveryResult> SendAsync(T message, MessageContext? context = null, CancellationToken cancellationToken = default)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            var stopwatch = Stopwatch.StartNew();

            try
            {
                var keyValue = KeyExtractor.ExtractKey(message, _entityModel);

                var kafkaMessage = new Message<object, object>
                {
                    Key = keyValue,
                    Value = message,
                    Headers = BuildHeaders(context),
                    Timestamp = new Timestamp(DateTime.UtcNow)
                };

                var topicPartition = context?.TargetPartition.HasValue == true
                    ? new TopicPartition(TopicName, new Partition(context.TargetPartition.Value))
                    : new TopicPartition(TopicName, Partition.Any);

                var deliveryResult = await _rawProducer.ProduceAsync(topicPartition, kafkaMessage, cancellationToken);
                stopwatch.Stop();

                UpdateSendStats(success: true, stopwatch.Elapsed);

                return new KafkaDeliveryResult
                {
                    Topic = deliveryResult.Topic,
                    Partition = deliveryResult.Partition.Value,
                    Offset = deliveryResult.Offset.Value,
                    Timestamp = deliveryResult.Timestamp.UtcDateTime,
                    Status = deliveryResult.Status,
                    Latency = stopwatch.Elapsed
                };
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
}