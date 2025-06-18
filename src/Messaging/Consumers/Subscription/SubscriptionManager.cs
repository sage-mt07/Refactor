using KsqlDsl.Messaging.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers.Subscription;

public class SubscriptionManager<T> : ISubscriptionManager<T> where T : class
{
    private readonly ConcurrentDictionary<string, SubscriptionInfo<T>> _activeSubscriptions = new();
    private readonly ILogger<SubscriptionManager<T>> _logger;
    private bool _disposed = false;

    public SubscriptionManager(ILogger<SubscriptionManager<T>> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<string> StartSubscriptionAsync(
        Func<T, MessageContext, Task> handler,
        SubscriptionOptions options,
        CancellationToken cancellationToken = default)
    {
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));
        if (options == null)
            throw new ArgumentNullException(nameof(options));

        var subscriptionId = Guid.NewGuid().ToString();

        var subscriptionInfo = new SubscriptionInfo<T>
        {
            Id = subscriptionId,
            StartedAt = DateTime.UtcNow,
            Options = options,
            Status = SubscriptionStatus.Active,
            MessagesProcessed = 0,
            LastMessageAt = DateTime.MinValue
        };

        _activeSubscriptions[subscriptionId] = subscriptionInfo;

        // 実際の購読処理はバックグラウンドで開始
        _ = Task.Run(async () => await ProcessSubscriptionAsync(subscriptionId, handler, cancellationToken), cancellationToken);

        _logger.LogInformation("Subscription started: {SubscriptionId} for {EntityType}", subscriptionId, typeof(T).Name);

        await Task.Delay(1, cancellationToken);
        return subscriptionId;
    }

    public async Task StopSubscriptionAsync(string subscriptionId)
    {
        if (_activeSubscriptions.TryRemove(subscriptionId, out var subscription))
        {
            subscription.Status = SubscriptionStatus.Stopped;
            _logger.LogInformation("Subscription stopped: {SubscriptionId} for {EntityType}", subscriptionId, typeof(T).Name);
        }

        await Task.Delay(1);
    }

    public async Task PauseSubscriptionAsync(string subscriptionId)
    {
        if (_activeSubscriptions.TryGetValue(subscriptionId, out var subscription))
        {
            subscription.Status = SubscriptionStatus.Paused;
            _logger.LogInformation("Subscription paused: {SubscriptionId} for {EntityType}", subscriptionId, typeof(T).Name);
        }

        await Task.Delay(1);
    }

    public async Task ResumeSubscriptionAsync(string subscriptionId)
    {
        if (_activeSubscriptions.TryGetValue(subscriptionId, out var subscription))
        {
            subscription.Status = SubscriptionStatus.Active;
            _logger.LogInformation("Subscription resumed: {SubscriptionId} for {EntityType}", subscriptionId, typeof(T).Name);
        }

        await Task.Delay(1);
    }

    public List<SubscriptionInfo<T>> GetActiveSubscriptions()
    {
        return _activeSubscriptions.Values.ToList();
    }

    public SubscriptionStatistics GetStatistics()
    {
        var subscriptions = _activeSubscriptions.Values.ToList();

        return new SubscriptionStatistics
        {
            ActiveSubscriptions = subscriptions.Count(s => s.Status == SubscriptionStatus.Active),
            TotalMessagesProcessed = subscriptions.Sum(s => s.MessagesProcessed),
            TotalErrors = subscriptions.Count(s => s.Status == SubscriptionStatus.Error),
            AverageProcessingTime = TimeSpan.Zero,
            LastUpdated = DateTime.UtcNow
        };
    }

    private async Task ProcessSubscriptionAsync(string subscriptionId, Func<T, MessageContext, Task> handler, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   _activeSubscriptions.TryGetValue(subscriptionId, out var subscription) &&
                   subscription.Status != SubscriptionStatus.Stopped)
            {
                if (subscription.Status == SubscriptionStatus.Paused)
                {
                    await Task.Delay(100, cancellationToken);
                    continue;
                }

                // 実際のメッセージ処理はここで実装
                // 簡略化のため、処理のプレースホルダー
                await Task.Delay(100, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Subscription processing cancelled: {SubscriptionId}", subscriptionId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Subscription processing error: {SubscriptionId}", subscriptionId);

            if (_activeSubscriptions.TryGetValue(subscriptionId, out var subscription))
            {
                subscription.Status = SubscriptionStatus.Error;
            }
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            foreach (var subscription in _activeSubscriptions.Values)
            {
                subscription.Status = SubscriptionStatus.Stopped;
            }

            _activeSubscriptions.Clear();
            _disposed = true;

            _logger.LogInformation("SubscriptionManager disposed for {EntityType}", typeof(T).Name);
        }
    }
}

internal class SubscriptionContext<T> where T : class
{
    public string Id { get; set; } = string.Empty;
    public Func<T, MessageContext, Task> Handler { get; set; } = default!;
    public SubscriptionOptions Options { get; set; } = default!;
    public SubscriptionStatus Status { get; set; }
    public DateTime StartedAt { get; set; }
    public long MessagesProcessed { get; set; }
    public DateTime LastMessageAt { get; set; }
    public CancellationTokenSource CancellationTokenSource { get; set; } = new();
}