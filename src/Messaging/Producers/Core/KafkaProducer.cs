
// =============================================================================
// src/Messaging/Producers/Core/KafkaProducer.cs
// =============================================================================

using Confluent.Kafka;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Core.Models;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Monitoring.Metrics;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
                stopwatch.Stop();
                UpdateSendStats(success: false, stopwatch.Elapsed);

                _logger.LogError(ex, "Failed to send message: {EntityType}", typeof(T).Name);

                // ✅ 正しい戻り値型で返す
                return new KafkaDeliveryResult
                {
                    Topic = TopicName,
                    Partition = -1,  // エラー時は無効な値
                    Offset = -1,     // エラー時は無効な値
                    Timestamp = DateTime.UtcNow,
                    Status = PersistenceStatus.NotPersisted,
                    Latency = stopwatch.Elapsed
                };
            }
        }



     
      


        public void Dispose()
        {
            if (!_disposed)
            {
                try
                {
                    // ✅ Producer用の適切なDispose処理
                    _rawProducer?.Flush(TimeSpan.FromSeconds(10));
                    _rawProducer?.Dispose();
                }
                catch (System.Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing producer: {EntityType}", typeof(T).Name);
                }
                _disposed = true;
            }
        }
    }
}