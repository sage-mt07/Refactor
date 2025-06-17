// =============================================================================
// Messaging/Producers/Core/KafkaProducer.cs - Phase 3: 機能分離後のProducer
// =============================================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KsqlDsl.Communication; // 既存インターフェース参照
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Modeling;
using Microsoft.Extensions.Logging;

namespace KsqlDsl.Messaging.Producers.Core
{
    /// <summary>
    /// Phase 3最適化版: 型安全Producer実装
    /// 設計理由：機能分離により責務を明確化し、既存Avroシリアライザーとの統合維持
    /// </summary>
    public class KafkaProducer<T> : IKafkaProducer<T> where T : class
    {
        private readonly IProducer<object, object> _rawProducer;
        private readonly ISerializer<object> _keySerializer;
        private readonly ISerializer<object> _valueSerializer;
        private readonly EntityModel _entityModel;
        private readonly IProducerMetricsCollector _metricsCollector;
        private readonly ILogger _logger;
        private readonly KafkaProducerStats _stats = new();
        private bool _disposed = false;

        public string TopicName { get; }

        // Phase 3: プール管理との統合用
        internal IProducer<object, object> RawProducer => _rawProducer;

        public KafkaProducer(
            IProducer<object, object> rawProducer,
            ISerializer<object> keySerializer,
            ISerializer<object> valueSerializer,
            string topicName,
            EntityModel entityModel,
            IProducerMetricsCollector metricsCollector,
            ILogger logger)
        {
            _rawProducer = rawProducer ?? throw new ArgumentNullException(nameof(rawProducer));
            _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
            TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _metricsCollector = metricsCollector ?? throw new ArgumentNullException(nameof(metricsCollector));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }


        public async Task<KafkaDeliveryResult> SendAsync(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            using var activity = StartSendActivity("send_single", context);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var messageContext = ConvertToMessageContext(context);
                var keyValue = ExtractKeyValue(message);

                var kafkaMessage = new Message<object, object>
                {
                    Key = keyValue,
                    Value = message,
                    Headers = BuildHeaders(messageContext),
                    Timestamp = new Timestamp(DateTime.UtcNow)
                };

                var topicPartition = messageContext?.TargetPartition.HasValue == true
                    ? new TopicPartition(TopicName, new Partition(messageContext.TargetPartition.Value))
                    : new TopicPartition(TopicName, Partition.Any);

                var deliveryResult = await _rawProducer.ProduceAsync(topicPartition, kafkaMessage, cancellationToken);
                stopwatch.Stop();

                _metricsCollector.RecordSend(success: true, stopwatch.Elapsed, deliveryResult.Message.Value?.ToString()?.Length ?? 0);
                UpdateStats(success: true, stopwatch.Elapsed);

                var result = new KafkaDeliveryResult
                {
                    Topic = deliveryResult.Topic,
                    Partition = deliveryResult.Partition.Value,
                    Offset = deliveryResult.Offset.Value,
                    Timestamp = deliveryResult.Timestamp.UtcDateTime,
                    Status = deliveryResult.Status,
                    Latency = stopwatch.Elapsed
                };

                activity?.SetTag("kafka.delivery.partition", result.Partition)
                        ?.SetTag("kafka.delivery.offset", result.Offset)
                        ?.SetStatus(ActivityStatusCode.Ok);

                return result;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _metricsCollector.RecordSend(success: false, stopwatch.Elapsed, 0);
                UpdateStats(success: false, stopwatch.Elapsed);
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Phase 3改良: バッチ最適化強化版
        /// </summary>
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
                    AllSuccessful = true
                };
            }

            using var activity = StartSendActivity("send_batch", ConvertToMessageContext(context));
            activity?.SetTag("kafka.batch.size", messageList.Count);

            var stopwatch = Stopwatch.StartNew();
            var results = new List<KafkaDeliveryResult>();
            var errors = new List<BatchDeliveryError>();

            try
            {
                // Phase 3: バッチ並列送信最適化
                var batchTasks = new List<Task<(int index, DeliveryResult<object, object>? result, Error? error)>>();

                for (int i = 0; i < messageList.Count; i++)
                {
                    var index = i;
                    var message = messageList[i];

                    var task = SendSingleMessageAsync(message, index, ConvertToMessageContext(context), cancellationToken);
                    batchTasks.Add(task);
                }

                // 全タスク完了待機
                var batchResults = await Task.WhenAll(batchTasks);
                stopwatch.Stop();

                // 結果集計
                foreach (var (index, result, error) in batchResults)
                {
                    if (error != null)
                    {
                        errors.Add(new BatchDeliveryError
                        {
                            MessageIndex = index,
                            Error = error,
                            OriginalMessage = messageList[index]
                        });
                    }
                    else if (result != null)
                    {
                        results.Add(new KafkaDeliveryResult
                        {
                            Topic = result.Topic,
                            Partition = result.Partition.Value,
                            Offset = result.Offset.Value,
                            Timestamp = result.Timestamp.UtcDateTime,
                            Status = result.Status,
                            Latency = stopwatch.Elapsed
                        });
                    }
                }

                // Phase 3: バッチメトリクス統合
                var totalBytes = messageList.Sum(m => m?.ToString()?.Length ?? 0);
                _metricsCollector.RecordBatch(messageList.Count, errors.Count == 0, stopwatch.Elapsed, totalBytes);
                UpdateBatchStats(messageList.Count, errors.Count == 0, stopwatch.Elapsed);

                var batchResult = new KafkaBatchDeliveryResult
                {
                    Topic = TopicName,
                    TotalMessages = messageList.Count,
                    SuccessfulCount = results.Count,
                    FailedCount = errors.Count,
                    Results = results,
                    Errors = errors,
                    TotalLatency = stopwatch.Elapsed
                };

                activity?.SetTag("kafka.batch.successful", results.Count)
                        ?.SetTag("kafka.batch.failed", errors.Count)
                        ?.SetStatus(batchResult.AllSuccessful ? ActivityStatusCode.Ok : ActivityStatusCode.Error);

                _logger.LogDebug("Batch sent: {EntityType} -> {Topic}, {SuccessCount}/{TotalCount} successful ({Duration}ms)",
                    typeof(T).Name, TopicName, results.Count, messageList.Count, stopwatch.ElapsedMilliseconds);

                return batchResult;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _metricsCollector.RecordBatch(messageList.Count, success: false, stopwatch.Elapsed, 0);
                UpdateBatchStats(messageList.Count, success: false, stopwatch.Elapsed);

                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

                _logger.LogError(ex, "Failed to send batch: {EntityType} -> {Topic}, {MessageCount} messages ({Duration}ms)",
                    typeof(T).Name, TopicName, messageList.Count, stopwatch.ElapsedMilliseconds);
                throw;
            }
        }

        /// <summary>
        /// 統計情報取得
        /// </summary>
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

        /// <summary>
        /// 保留メッセージフラッシュ
        /// </summary>
        public async Task FlushAsync(TimeSpan timeout)
        {
            try
            {
                _rawProducer.Flush(timeout);
                await Task.Delay(1); // 非同期メソッドの形式保持

                _logger.LogTrace("Producer flushed: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to flush producer: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
                throw;
            }
        }

        // =============================================================================
        // Private Helper Methods
        // =============================================================================

        private async Task<(int index, DeliveryResult<object, object>? result, Error? error)> SendSingleMessageAsync(
            T message, int index, MessageContext? context, CancellationToken cancellationToken)
        {
            try
            {
                var keyValue = ExtractKeyValue(message);
                var kafkaMessage = new Message<object, object>
                {
                    Key = keyValue,
                    Value = message,
                    Headers = BuildHeaders(context),
                    Timestamp = new Timestamp(DateTime.UtcNow)
                };

                var deliveryResult = await _rawProducer.ProduceAsync(TopicName, kafkaMessage, cancellationToken);
                return (index, deliveryResult, null);
            }
            catch (ProduceException<object, object> ex)
            {
                return (index, null, ex.Error);
            }
        }

        private object? ExtractKeyValue(T message)
        {
            try
            {
                return KsqlDsl.Avro.KeyExtractor.ExtractKey(message, _entityModel);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to extract key from message: {EntityType}", typeof(T).Name);
                return null;
            }
        }

        private Headers? BuildHeaders(MessageContext? context)
        {
            if (