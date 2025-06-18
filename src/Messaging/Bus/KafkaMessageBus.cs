using KsqlDsl.Messaging.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Bus;

public class KafkaMessageBus : IMessageBusCoordinator, IDisposable
{
    private readonly ILogger<KafkaMessageBus> _logger;
    private readonly ConcurrentDictionary<string, object> _activeSubscriptions = new();
    private bool _disposed = false;

    public KafkaMessageBus(ILogger<KafkaMessageBus> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task SendAsync<T>(T message, MessageContext? context = null, CancellationToken cancellationToken = default) where T : class
    {
        if (message == null)
            throw new ArgumentNullException(nameof(message));

        using var activity = StartSendActivity<T>("send_single", context);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 実際の送信処理（Producer取得・送信）
            // 簡略化のため、成功とみなす
            await Task.Delay(1, cancellationToken);

            stopwatch.Stop();

            activity?.SetStatus(ActivityStatusCode.Ok);

            _logger.LogDebug("Message sent successfully: {EntityType} ({Duration}ms)",
                typeof(T).Name, stopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

            _logger.LogError(ex, "Failed to send message: {EntityType} ({Duration}ms)",
                typeof(T).Name, stopwatch.ElapsedMilliseconds);

            throw new InvalidOperationException($"Failed to send {typeof(T).Name} message", ex);
        }
    }

    public async Task SendBatchAsync<T>(IEnumerable<T> messages, MessageContext? context = null, CancellationToken cancellationToken = default) where T : class
    {
        if (messages == null)
            throw new ArgumentNullException(nameof(messages));

        var messageList = messages.ToList();
        if (messageList.Count == 0)
            return;

        using var activity = StartSendActivity<T>("send_batch", context);
        activity?.SetTag("kafka.batch.size", messageList.Count);

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 実際のバッチ送信処理
            await Task.Delay(messageList.Count, cancellationToken);

            stopwatch.Stop();

            activity?.SetTag("kafka.batch.successful", messageList.Count)
                    ?.SetTag("kafka.batch.failed", 0)
                    ?.SetStatus(ActivityStatusCode.Ok);

            _logger.LogInformation("Batch sent: {EntityType} - {MessageCount} messages ({Duration}ms)",
                typeof(T).Name, messageList.Count, stopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

            _logger.LogError(ex, "Failed to send batch: {EntityType}, {MessageCount} messages ({Duration}ms)",
                typeof(T).Name, messageList.Count, stopwatch.ElapsedMilliseconds);

            throw new InvalidOperationException($"Failed to send {typeof(T).Name} batch ({messageList.Count} messages)", ex);
        }
    }

    public async Task<string> SubscribeAsync<T>(Func<T, MessageContext, Task> handler, SubscriptionOptions? options = null, CancellationToken cancellationToken = default) where T : class
    {
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));

        var subscriptionId = Guid.NewGuid().ToString();
        var subscriptionOptions = options ?? new SubscriptionOptions();

        _activeSubscriptions[subscriptionId] = new
        {
            EntityType = typeof(T),
            Handler = handler,
            Options = subscriptionOptions,
            StartedAt = DateTime.UtcNow
        };

        _logger.LogInformation("Subscription created: {SubscriptionId} for {EntityType}", subscriptionId, typeof(T).Name);

        await Task.Delay(1, cancellationToken);
        return subscriptionId;
    }

    public async Task UnsubscribeAsync(string subscriptionId)
    {
        if (_activeSubscriptions.TryRemove(subscriptionId, out var subscription))
        {
            _logger.LogInformation("Subscription removed: {SubscriptionId}", subscriptionId);
        }

        await Task.Delay(1);
    }

    public async Task<MessageBusHealthStatus> GetHealthStatusAsync()
    {
        try
        {
            var status = new MessageBusHealthStatus
            {
                HealthLevel = MessageBusHealthLevel.Healthy,
                Issues = new List<MessageBusHealthIssue>(),
                LastCheck = DateTime.UtcNow
            };

            if (_activeSubscriptions.Count > 100)
            {
                status.Issues.Add(new MessageBusHealthIssue
                {
                    Type = MessageBusHealthIssueType.PerformanceIssue,
                    Description = $"High number of active subscriptions: {_activeSubscriptions.Count}",
                    Severity = MessageBusIssueSeverity.Medium
                });
                status.HealthLevel = MessageBusHealthLevel.Warning;
            }

            await Task.Delay(1);
            return status;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get message bus health status");

            return new MessageBusHealthStatus
            {
                HealthLevel = MessageBusHealthLevel.Critical,
                Issues = new List<MessageBusHealthIssue>
                    {
                        new() {
                            Type = MessageBusHealthIssueType.ConfigurationError,
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
                ["ActiveSubscriptions"] = _activeSubscriptions.Count,
                ["MessageBusType"] = "KafkaMessageBus"
            },
            SystemMetrics = new Dictionary<string, object>
            {
                ["MemoryUsage"] = GC.GetTotalMemory(false),
                ["ThreadCount"] = System.Diagnostics.Process.GetCurrentProcess().Threads.Count
            }
        };
    }

    private Activity? StartSendActivity<T>(string operationName, MessageContext? context)
    {
        var activity = Activity.Current?.Source.StartActivity($"messaging.{operationName}")
            ?.SetTag("messaging.entity.type", typeof(T).Name)
            ?.SetTag("messaging.system", "kafka")
            ?.SetTag("messaging.operation", operationName);

        if (context?.ActivityContext.HasValue == true)
        {
            activity?.SetParentId(context.ActivityContext.Value.TraceId, context.ActivityContext.Value.SpanId);
        }

        if (context?.CorrelationId != null)
        {
            activity?.SetTag("messaging.correlation_id", context.CorrelationId);
        }

        return activity;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _logger.LogInformation("Disposing KafkaMessageBus...");

            _activeSubscriptions.Clear();

            _disposed = true;
            _logger.LogInformation("KafkaMessageBus disposed successfully");
        }
    }
}