using Confluent.Kafka;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Core.Models;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Producers.Core;
using KsqlDsl.Messaging.Producers.Exception;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Producers.Core
{
    /// <summary>
    /// 統合型安全Producer - TypedKafkaProducer + KafkaProducer統合版
    /// 設計理由: Pool削除、Confluent.Kafka完全委譲、シンプル化
    /// </summary>
    public class KafkaProducer<T> : IKafkaProducer<T> where T : class
    {
        private readonly IProducer<object, object> _producer;
        private readonly ISerializer<object> _keySerializer;
        private readonly ISerializer<object> _valueSerializer;
        private readonly EntityModel _entityModel;
        private readonly ILogger? _logger;
        private bool _disposed = false;

        public string TopicName { get; }

        public KafkaProducer(
            IProducer<object, object> producer,
            ISerializer<object> keySerializer,
            ISerializer<object> valueSerializer,
            string topicName,
            EntityModel entityModel,
            ILoggerFactory? loggerFactory = null)
        {
            _producer = producer ?? throw new ArgumentNullException(nameof(producer));
            _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
            TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _logger = loggerFactory.CreateLoggerOrNull<KafkaProducer<T>>();
        }

        public async Task<KafkaDeliveryResult> SendAsync(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            try
            {
                var keyValue = KeyExtractor.ExtractKeyValue(message, _entityModel);

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

                var deliveryResult = await _producer.ProduceAsync(topicPartition, kafkaMessage, cancellationToken);

                _logger?.LogDebug("Message sent: {EntityType} -> {Topic}, Partition: {Partition}, Offset: {Offset}",
                    typeof(T).Name, deliveryResult.Topic, deliveryResult.Partition.Value, deliveryResult.Offset.Value);

                return new KafkaDeliveryResult
                {
                    Topic = deliveryResult.Topic,
                    Partition = deliveryResult.Partition.Value,
                    Offset = deliveryResult.Offset.Value,
                    Timestamp = deliveryResult.Timestamp.UtcDateTime,
                    Status = deliveryResult.Status,
                    Latency = TimeSpan.Zero // Confluent.Kafkaの統計に委譲
                };
            }
            catch (System.Exception ex)
            {
                _logger?.LogError(ex, "Failed to send message: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
                throw;
            }
        }

        public async Task<KafkaBatchDeliveryResult> SendBatchAsync(IEnumerable<T> messages, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
        {
            if (messages == null)
                throw new ArgumentNullException(nameof(messages));

            var messageList = messages.ToList();
            if (messageList.Count == 0)
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

            var results = new List<KafkaDeliveryResult>();
            var errors = new List<BatchDeliveryError>();

            var tasks = messageList.Select(async (message, index) =>
            {
                try
                {
                    var result = await SendAsync(message, context, cancellationToken);
                    return new { Index = index, Result = result, Error = (Error?)null };
                }
                catch (ProduceException<object, object> ex)
                {
                    return new { Index = index, Result = (KafkaDeliveryResult?)null, Error = ex.Error };
                }
            });

            var taskResults = await Task.WhenAll(tasks);

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
                TotalLatency = TimeSpan.Zero // Confluent.Kafkaの統計に委譲
            };
        }



        public async Task FlushAsync(TimeSpan timeout)
        {
            try
            {
                _producer.Flush(timeout);
                await Task.Delay(1);
                _logger?.LogTrace("Producer flushed: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
            }
            catch (System.Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to flush producer: {EntityType} -> {Topic}", typeof(T).Name, TopicName);
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

        public void Dispose()
        {
            if (!_disposed)
            {
                try
                {
                    _producer?.Flush(TimeSpan.FromSeconds(5));
                    _producer?.Dispose();
                }
                catch (System.Exception ex)
                {
                    _logger?.LogWarning(ex, "Error disposing producer: {EntityType}", typeof(T).Name);
                }
                _disposed = true;
            }
        }
    }
}