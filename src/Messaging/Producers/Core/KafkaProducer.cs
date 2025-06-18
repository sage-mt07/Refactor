// =============================================================================
// src/Messaging/Producers/Core/KafkaProducer.cs
// =============================================================================

using Confluent.Kafka;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Core.Models;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Producers.Exception;
using KsqlDsl.Monitoring.Metrics;
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

        public async Task<KafkaDeliveryResult> SendAsync(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
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
            catch (System.Exception ex)
            {
                stopwatch.Stop();
                UpdateSendStats(success: false, stopwatch.Elapsed);

                _logger.LogError(ex, "Failed to send message: {EntityType}", typeof(T).Name);

                return new KafkaDeliveryResult
                {
                    Topic = TopicName,
                    Partition = -1,
                    Offset = -1,
                    Timestamp = DateTime.UtcNow,
                    Status = PersistenceStatus.NotPersisted,
                    Latency = stopwatch.Elapsed
                };
            }
        }

        public async Task<KafkaBatchDeliveryResult> SendBatchAsync(IEnumerable<T> messages, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
        {
            if (messages == null)
                throw new ArgumentNullException(nameof(messages));

            var messageList = messages.ToList();
            if (messageList.Count == 0)
            {
                return new KafkaBatchDeliveryResult
                {
                    Topic = TopicName,
                    TotalMessages = 0,
                    SuccessfulCount = 0,
                    FailedCount = 0,
                    Results = new List<KafkaDeliveryResult>(),
                    Errors = new List<BatchDeliveryError>(),
                    TotalLatency = TimeSpan.Zero
                };
            }

            var stopwatch = Stopwatch.StartNew();
            var results = new List<KafkaDeliveryResult>();
            var errors = new List<BatchDeliveryError>();

            try
            {
                var tasks = messageList.Select(async (message, index) =>
                {
                    try
                    {
                        var result = await SendAsync(message, context, cancellationToken);
                        return new { Index = index, Result = result, Error = (Error?)null };
                    }
                    catch (System.Exception ex)
                    {
                        return new { Index = index, Result = (KafkaDeliveryResult?)null, Error = new Error(ErrorCode.Local_Application, ex.Message, false) };
                    }
                });

                var taskResults = await Task.WhenAll(tasks);
                stopwatch.Stop();

                foreach (var taskResult in taskResults)
                {
                    if (taskResult.Error != null)
                    {
                        errors.Add(new BatchDeliveryError
                        {
                            MessageIndex = taskResult.Index,
                            Error = taskResult.Error,
                            OriginalMessage = messageList[taskResult.Index]
                        });
                    }
                    else if (taskResult.Result != null)
                    {
                        results.Add(taskResult.Result);
                    }
                }

                return new KafkaBatchDeliveryResult
                {
                    Topic = TopicName,
                    TotalMessages = messageList.Count,
                    SuccessfulCount = results.Count,
                    FailedCount = errors.Count,
                    Results = results,
                    Errors = errors,
                    TotalLatency = stopwatch.Elapsed
                };
            }
            catch (System.Exception ex)
            {
                stopwatch.Stop();

                _logger.LogError(ex, "Failed to send batch: {EntityType}", typeof(T).Name);
                throw;
            }
        }

        // ✅ 不足していたGetStats()メソッドを実装
        public KafkaProducerStats GetStats()
        {
            lock (_stats)
            {
                return new KafkaProducerStats
                {
                    TotalMessagesSent = _stats.TotalMessagesSent,
                    SuccessfulMessages = _stats.SuccessfulMessages,
                    FailedMessages = _stats.FailedMessages,
                    AverageLatency = _stats.AverageLatency,
                    MinLatency = _stats.MinLatency,
                    MaxLatency = _stats.MaxLatency,
                    LastMessageSent = _stats.LastMessageSent,
                    TotalBytesSent = _stats.TotalBytesSent,
                    MessagesPerSecond = _stats.MessagesPerSecond
                };
            }
        }

        // ✅ 不足していたFlushAsync()メソッドを実装
        public async Task FlushAsync(TimeSpan timeout)
        {
            try
            {
                _rawProducer.Flush(timeout);
                await Task.Delay(1);
                _logger.LogTrace("Producer flushed: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
            }
            catch (System.Exception ex)
            {
                _logger.LogWarning(ex, "Failed to flush producer: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
                throw;
            }
        }

        private Headers? BuildHeaders(KafkaMessageContext? context)
        {
            if (context?.Headers == null || !context.Headers.Any())
                return null;

            var headers = new Headers();
            foreach (var kvp in context.Headers)
            {
                if (kvp.Value != null)
                {
                    var valueBytes = System.Text.Encoding.UTF8.GetBytes(kvp.Value.ToString() ?? "");
                    headers.Add(kvp.Key, valueBytes);
                }
            }

            return headers;
        }

        private void UpdateSendStats(bool success, TimeSpan latency)
        {
            lock (_stats)
            {
                _stats.TotalMessagesSent++;

                if (success)
                    _stats.SuccessfulMessages++;
                else
                    _stats.FailedMessages++;

                if (_stats.MinLatency == TimeSpan.Zero || latency < _stats.MinLatency)
                    _stats.MinLatency = latency;
                if (latency > _stats.MaxLatency)
                    _stats.MaxLatency = latency;

                if (_stats.TotalMessagesSent == 1)
                {
                    _stats.AverageLatency = latency;
                }
                else
                {
                    var totalMs = _stats.AverageLatency.TotalMilliseconds * (_stats.TotalMessagesSent - 1) + latency.TotalMilliseconds;
                    _stats.AverageLatency = TimeSpan.FromMilliseconds(totalMs / _stats.TotalMessagesSent);
                }

                _stats.LastMessageSent = DateTime.UtcNow;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                try
                {
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