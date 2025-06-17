using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KsqlDsl.Communication;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Producers.Core;
using KsqlDsl.Messaging.Consumers.Subscription;
using Microsoft.Extensions.Logging;

namespace KsqlDsl.Messaging.Bus
{
    public class KafkaMessageBus : IMessageBusCoordinator
    {
        private readonly IKafkaProducerManager _producerManager;
        private readonly IKafkaConsumerManager _consumerManager;
        private readonly Dictionary<Type, object> _subscriptionManagers = new();
        private readonly ILogger<KafkaMessageBus> _logger;
        private readonly BusDiagnostics _diagnostics;
        private bool _disposed = false;

        public KafkaMessageBus(
            IKafkaProducerManager producerManager,
            IKafkaConsumerManager consumerManager,
            ILogger<KafkaMessageBus> logger)
        {
            _producerManager = producerManager ?? throw new ArgumentNullException(nameof(producerManager));
            _consumerManager = consumerManager ?? throw new ArgumentNullException(nameof(consumerManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _diagnostics = new BusDiagnostics();
        }

        public async Task SendAsync<T>(T message, MessageContext? context = null, CancellationToken cancellationToken = default) where T : class
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            using var activity = StartActivity("message_bus_send", typeof(T).Name);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var producer = await _producerManager.GetProducerAsync<T>();
                try
                {
                    var kafkaContext = ConvertToKafkaContext(context);
                    var result = await producer.SendAsync(message, kafkaContext, cancellationToken);

                    stopwatch.Stop();
                    _diagnostics.RecordSend(success: true, stopwatch.Elapsed);

                    activity?.SetTag("kafka.delivery.partition", result.Partition)
                            ?.SetTag("kafka.delivery.offset", result.Offset)
                            ?.SetStatus(ActivityStatusCode.Ok);
                }
                finally
                {
                    _producerManager.ReturnProducer(producer);
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _diagnostics.RecordSend(success: false, stopwatch.Elapsed);

                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

                _logger.LogError(ex, "Failed to send message via MessageBus: {EntityType}", typeof(T).Name);
                throw;
            }
        }

        public async Task SendBatchAsync<T>(IEnumerable<T> messages, MessageContext? context = null, CancellationToken cancellationToken = default) where T : class
        {
            if (messages == null)
                throw new ArgumentNullException(nameof(messages));

            var messageList = messages.ToList();
            if (messageList.Count == 0)
                return;

            using var activity = StartActivity("message_bus_send_batch", typeof(T).Name);
            activity?.SetTag("kafka.batch.size", messageList.Count);

            var stopwatch = Stopwatch.StartNew();

            try
            {
                var kafkaContext = ConvertToKafkaContext(context);
                var batchResult = await _producerManager.SendBatchOptimizedAsync(messageList, kafkaContext, cancellationToken);

                stopwatch.Stop();
                _diagnostics.RecordBatch(messageList.Count, batchResult.AllSuccessful, stopwatch.Elapsed);

                activity?.SetTag("kafka.batch.successful", batchResult.SuccessfulCount)
                        ?.SetTag("kafka.batch.failed", batchResult.FailedCount)
                        ?.SetStatus(batchResult.AllSuccessful ? ActivityStatusCode.Ok : ActivityStatusCode.Error);

                if (!batchResult.AllSuccessful)
                {
                    throw new KafkaBatchSendException($"Batch send partially failed: {batchResult.FailedCount}/{messageList.Count} messages failed", batchResult);
                }
            }
            catch (Exception ex) when (!(ex is KafkaBatchSendException))
            {
                stopwatch.Stop();
                _diagnostics.RecordBatch(messageList.Count, success: false, stopwatch.Elapsed);

                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

                _logger.LogError(ex, "Failed to send batch via MessageBus: {EntityType}, {MessageCount} messages", typeof(T).Name, messageList.Count);
                throw;
            }
        }

        public async Task<string> SubscribeAsync<T>(Func<T, MessageContext, Task> handler, SubscriptionOptions? options = null, CancellationToken cancellationToken = default) where T : class
        {
            if (handler == null)
                throw new ArgumentNullException(nameof(handler));

            var subscriptionManager = GetOrCreateSubscriptionManager<T>();
            var subscriptionOptions = options ?? new SubscriptionOptions();

            var subscriptionId = await subscriptionManager.StartSubscriptionAsync(handler, subscriptionOptions, cancellationToken);

            _diagnostics.RecordSubscription<T>(subscriptionId, true);

            _logger.LogInformation("Subscription created via MessageBus: {SubscriptionId} for {EntityType}", subscriptionId, typeof(T).Name);

            return subscriptionId;
        }

        public async Task UnsubscribeAsync(string subscriptionId)
        {
            if (string.IsNullOrEmpty(subscriptionId))
                throw new ArgumentException("Subscription ID cannot be null or empty", nameof(subscriptionId));

            foreach (var kvp in _subscriptionManagers)
            {
                var manager = kvp.Value;
                if (manager is ISubscriptionManager<object> genericManager)
                {
                    try
                    {
                        await genericManager.StopSubscriptionAsync(subscriptionId);
                        _diagnostics.RecordUnsubscribe(subscriptionId, true);

                        _logger.LogInformation("Subscription stopped via MessageBus: {SubscriptionId}", subscriptionId);
                        return;
                    }
                    catch
                    {
                        continue;
                    }
                }
            }

            _diagnostics.RecordUnsubscribe(subscriptionId, false);
            _logger.LogWarning("Subscription not found for unsubscribe: {SubscriptionId}", subscriptionId);
        }

        public async Task<MessageBusHealthStatus> GetHealthStatusAsync()
        {
            try
            {
                var producerHealth = await _producerManager.GetHealthStatusAsync();
                var consumerHealth = await _consumerManager.GetHealthStatusAsync();

                var healthLevel = DetermineOverallHealth(producerHealth, consumerHealth);

                var status = new MessageBusHealthStatus
                {
                    HealthLevel = healthLevel,
                    Issues = new List<MessageBusHealthIssue>(),
                    LastCheck = DateTime.UtcNow
                };

                if (producerHealth.HealthLevel != ProducerHealthLevel.Healthy)
                {
                    status.Issues.Add(new MessageBusHealthIssue
                    {
                        Type = MessageBusHealthIssueType.ProducerIssue,
                        Description = $"Producer health: {producerHealth.HealthLevel}",
                        Severity = MapToMessageBusSeverity(producerHealth.HealthLevel)
                    });
                }

                if (consumerHealth.HealthLevel != ConsumerHealthLevel.Healthy)
                {
                    status.Issues.Add(new MessageBusHealthIssue
                    {
                        Type = MessageBusHealthIssueType.ConsumerIssue,
                        Description = $"Consumer health: {consumerHealth.HealthLevel}",
                        Severity = MapToMessageBusSeverity(consumerHealth.HealthLevel)
                    });
                }

                return status;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to get MessageBus health status");
                return new MessageBusHealthStatus
                {
                    HealthLevel = MessageBusHealthLevel.Critical,
                    Issues = new List<MessageBusHealthIssue>
                    {
                        new() {
                            Type = MessageBusHealthIssueType.PerformanceIssue,
                            Description = $"Health check failed: {ex.Message}",
                            Severity = MessageBusIssueSeverity.Critical
                        }
                    },
                    LastCheck = DateTime.UtcNow
                };
            }
        }

        public MessageBusDiagnostics GetDiagnostics()
        {
            return new MessageBusDiagnostics
            {
                GeneratedAt = DateTime.UtcNow,
                Configuration = new Dictionary<string, object>
                {
                    ["ActiveSubscriptionManagers"] = _subscriptionManagers.Count,
                    ["TotalSentMessages"] = _diagnostics.TotalSentMessages,
                    ["TotalBatches"] = _diagnostics.TotalBatches,
                    ["ActiveSubscriptions"] = _diagnostics.ActiveSubscriptions
                },
                ProducerPoolStats = _producerManager.GetPerformanceStats().ConvertToPoolStats(),
                ConsumerPoolStats = _consumerManager.GetPerformanceStats().ConvertToPoolStats(),
                SubscriptionStats = GetCombinedSubscriptionStats(),
                SystemMetrics = new Dictionary<string, object>
                {
                    ["MemoryUsage"] = GC.GetTotalMemory(false),
                    ["ThreadCount"] = System.Diagnostics.Process.GetCurrentProcess().Threads.Count,
                    ["Uptime"] = _diagnostics.StartTime.Subtract(DateTime.UtcNow)
                }
            };
        }

        private ISubscriptionManager<T> GetOrCreateSubscriptionManager<T>() where T : class
        {
            var entityType = typeof(T);

            if (_subscriptionManagers.TryGetValue(entityType, out var existingManager))
            {
                return (ISubscriptionManager<T>)existingManager;
            }

            var logger = _logger; // 実際は専用のロガーを作成
            var newManager = new SubscriptionManager<T>(logger);
            _subscriptionManagers[entityType] = newManager;

            return newManager;
        }

        private KafkaMessageContext? ConvertToKafkaContext(MessageContext? context)
        {
            if (context == null) return null;

            return new KafkaMessageContext
            {
                MessageId = context.MessageId,
                CorrelationId = context.CorrelationId,
                TargetPartition = context.TargetPartition,
                Headers = context.Headers,
                Timeout = context.Timeout,
                Tags = context.Tags,
                ActivityContext = context.ActivityContext
            };
        }

        private Activity? StartActivity(string operationName, string entityType)
        {
            return Activity.Current?.Source.StartActivity($"kafka.{operationName}")
                ?.SetTag("kafka.entity.type", entityType)
                ?.SetTag("messaging.system", "kafka")
                ?.SetTag("messaging.operation", operationName);
        }

        private MessageBusHealthLevel DetermineOverallHealth(ProducerHealthStatus producerHealth, ConsumerHealthStatus consumerHealth)
        {
            if (producerHealth.HealthLevel == ProducerHealthLevel.Critical ||
                consumerHealth.HealthLevel == ConsumerHealthLevel.Critical)
            {
                return MessageBusHealthLevel.Critical;
            }

            if (producerHealth.HealthLevel == ProducerHealthLevel.Warning ||
                consumerHealth.HealthLevel == ConsumerHealthLevel.Warning)
            {
                return MessageBusHealthLevel.Warning;
            }

            return MessageBusHealthLevel.Healthy;
        }

        private MessageBusIssueSeverity MapToMessageBusSeverity(object healthLevel)
        {
            return healthLevel switch
            {
                ProducerHealthLevel.Critical => MessageBusIssueSeverity.Critical,
                ProducerHealthLevel.Warning => MessageBusIssueSeverity.Medium,
                ConsumerHealthLevel.Critical => MessageBusIssueSeverity.Critical,
                ConsumerHealthLevel.Warning => MessageBusIssueSeverity.Medium,
                _ => MessageBusIssueSeverity.Low
            };
        }

        private SubscriptionStatistics GetCombinedSubscriptionStats()
        {
            var totalActive = 0;
            var totalMessages = 0L;
            var totalErrors = 0L;
            var avgProcessingTime = TimeSpan.Zero;

            foreach (var manager in _subscriptionManagers.Values)
            {
                if (manager is ISubscriptionManager<object> genericManager)
                {
                    var stats = genericManager.GetStatistics();
                    totalActive += stats.ActiveSubscriptions;
                    totalMessages += stats.TotalMessagesProcessed;
                    totalErrors += stats.TotalErrors;
                }
            }

            return new SubscriptionStatistics
            {
                ActiveSubscriptions = totalActive,
                TotalMessagesProcessed = totalMessages,
                TotalErrors = totalErrors,
                AverageProcessingTime = avgProcessingTime,
                LastUpdated = DateTime.UtcNow
            };
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                foreach (var manager in _subscriptionManagers.Values)
                {
                    if (manager is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }

                _subscriptionManagers.Clear();

                _logger.LogInformation("KafkaMessageBus disposed successfully");

                _disposed = true;
            }
        }
    }
}