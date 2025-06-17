using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KsqlDsl.Communication;
using KsqlDsl.Messaging.Abstractions;
using Microsoft.Extensions.Logging;

namespace KsqlDsl.Messaging.Consumers.Subscription
{
    public class SubscriptionManager<T> : ISubscriptionManager<T> where T : class
    {
        private readonly ConcurrentDictionary<string, SubscriptionContext<T>> _activeSubscriptions = new();
        private readonly ILogger<SubscriptionManager<T>> _logger;
        private readonly SubscriptionStatistics _statistics = new();
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

            var subscriptionId = GenerateSubscriptionId();
            var subscriptionContext = new SubscriptionContext<T>
            {
                Id = subscriptionId,
                Handler = handler,
                Options = options,
                Status = SubscriptionStatus.Active,
                StartedAt = DateTime.UtcNow,
                CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            };

            _activeSubscriptions[subscriptionId] = subscriptionContext;

            _ = Task.Run(async () => await ProcessSubscriptionAsync(subscriptionContext), cancellationToken);

            lock (_statistics)
            {
                _statistics.ActiveSubscriptions++;
            }

            _logger.LogInformation("Subscription started: {SubscriptionId} for {EntityType}", subscriptionId, typeof(T).Name);

            await Task.Delay(1);
            return subscriptionId;
        }

        public async Task StopSubscriptionAsync(string subscriptionId)
        {
            if (string.IsNullOrEmpty(subscriptionId))
                throw new ArgumentException("Subscription ID cannot be null or empty", nameof(subscriptionId));

            if (_activeSubscriptions.TryRemove(subscriptionId, out var subscription))
            {
                subscription.Status = SubscriptionStatus.Stopped;
                subscription.CancellationTokenSource.Cancel();

                lock (_statistics)
                {
                    _statistics.ActiveSubscriptions--;
                }

                _logger.LogInformation("Subscription stopped: {SubscriptionId}", subscriptionId);
            }

            await Task.Delay(1);
        }

        public async Task PauseSubscriptionAsync(string subscriptionId)
        {
            if (string.IsNullOrEmpty(subscriptionId))
                throw new ArgumentException("Subscription ID cannot be null or empty", nameof(subscriptionId));

            if (_activeSubscriptions.TryGetValue(subscriptionId, out var subscription))
            {
                subscription.Status = SubscriptionStatus.Paused;
                _logger.LogInformation("Subscription paused: {SubscriptionId}", subscriptionId);
            }

            await Task.Delay(1);
        }

        public async Task ResumeSubscriptionAsync(string subscriptionId)
        {
            if (string.IsNullOrEmpty(subscriptionId))
                throw new ArgumentException("Subscription ID cannot be null or empty", nameof(subscriptionId));

            if (_activeSubscriptions.TryGetValue(subscriptionId, out var subscription))
            {
                subscription.Status = SubscriptionStatus.Active;
                _logger.LogInformation("Subscription resumed: {SubscriptionId}", subscriptionId);
            }

            await Task.Delay(1);
        }

        public List<SubscriptionInfo<T>> GetActiveSubscriptions()
        {
            return _activeSubscriptions.Values
                .Where(s => s.Status != SubscriptionStatus.Stopped)
                .Select(s => new SubscriptionInfo<T>
                {
                    Id = s.Id,
                    StartedAt = s.StartedAt,
                    Options = s.Options,
                    Status = s.Status,
                    MessagesProcessed = s.MessagesProcessed,
                    LastMessageAt = s.LastMessageAt
                })
                .ToList();
        }

        public SubscriptionStatistics GetStatistics()
        {
            lock (_statistics)
            {
                return new SubscriptionStatistics
                {
                    ActiveSubscriptions = _statistics.ActiveSubscriptions,
                    TotalMessagesProcessed = _statistics.TotalMessagesProcessed,
                    TotalErrors = _statistics.TotalErrors,
                    AverageProcessingTime = _statistics.AverageProcessingTime,
                    LastUpdated = DateTime.UtcNow
                };
            }
        }

        private async Task ProcessSubscriptionAsync(SubscriptionContext<T> subscriptionContext)
        {
            var cancellationToken = subscriptionContext.CancellationTokenSource.Token;

            try
            {
                while (!cancellationToken.IsCancellationRequested &&
                       subscriptionContext.Status != SubscriptionStatus.Stopped)
                {
                    if (subscriptionContext.Status == SubscriptionStatus.Paused)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                        continue;
                    }

                    try
                    {
                        var dummyMessage = CreateDummyMessage();
                        var messageContext = new MessageContext
                        {
                            MessageId = Guid.NewGuid().ToString(),
                            CorrelationId = subscriptionContext.Id
                        };

                        var processingStart = DateTime.UtcNow;
                        await subscriptionContext.Handler(dummyMessage, messageContext);
                        var processingTime = DateTime.UtcNow - processingStart;

                        subscriptionContext.MessagesProcessed++;
                        subscriptionContext.LastMessageAt = DateTime.UtcNow;

                        lock (_statistics)
                        {
                            _statistics.TotalMessagesProcessed++;
                            UpdateAverageProcessingTime(processingTime);
                        }

                        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        subscriptionContext.Status = SubscriptionStatus.Error;

                        lock (_statistics)
                        {
                            _statistics.TotalErrors++;
                        }

                        _logger.LogError(ex, "Error processing message in subscription: {SubscriptionId}", subscriptionContext.Id);

                        if (subscriptionContext.Options.StopOnError)
                        {
                            break;
                        }

                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                        subscriptionContext.Status = SubscriptionStatus.Active;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Subscription cancelled: {SubscriptionId}", subscriptionContext.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Subscription processing error: {SubscriptionId}", subscriptionContext.Id);
            }
            finally
            {
                _activeSubscriptions.TryRemove(subscriptionContext.Id, out _);

                lock (_statistics)
                {
                    _statistics.ActiveSubscriptions--;
                }
            }
        }

        private T CreateDummyMessage()
        {
            return Activator.CreateInstance<T>();
        }

        private void UpdateAverageProcessingTime(TimeSpan processingTime)
        {
            if (_statistics.TotalMessagesProcessed == 1)
            {
                _statistics.AverageProcessingTime = processingTime;
            }
            else
            {
                var totalMs = _statistics.AverageProcessingTime.TotalMilliseconds * (_statistics.TotalMessagesProcessed - 1) + processingTime.TotalMilliseconds;
                _statistics.AverageProcessingTime = TimeSpan.FromMilliseconds(totalMs / _statistics.TotalMessagesProcessed);
            }
        }

        private string GenerateSubscriptionId()
        {
            return $"{typeof(T).Name}_{Guid.NewGuid():N}";
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                foreach (var subscription in _activeSubscriptions.Values)
                {
                    subscription.CancellationTokenSource.Cancel();
                }

                _activeSubscriptions.Clear();

                _logger.LogInformation("SubscriptionManager disposed for {EntityType}", typeof(T).Name);

                _disposed = true;
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
}