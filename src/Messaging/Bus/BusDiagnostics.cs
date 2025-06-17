using KsqlDsl.Communication;
using KsqlDsl.Messaging.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace KsqlDsl.Messaging.Bus
{
    public class BusDiagnostics
    {
        private readonly ConcurrentDictionary<string, object> _metrics = new();
        private long _totalSentMessages;
        private long _totalBatches;
        private long _totalErrors;
        private long _activeSubscriptions;

        public DateTime StartTime { get; } = DateTime.UtcNow;
        public long TotalSentMessages => _totalSentMessages;
        public long TotalBatches => _totalBatches;
        public long TotalErrors => _totalErrors;
        public long ActiveSubscriptions => _activeSubscriptions;

        public void RecordSend(bool success, TimeSpan duration)
        {
            Interlocked.Increment(ref _totalSentMessages);

            if (!success)
            {
                Interlocked.Increment(ref _totalErrors);
            }

            _metrics.AddOrUpdate("last_send_duration", duration, (k, v) => duration);
            _metrics.AddOrUpdate("last_send_success", success, (k, v) => success);
            _metrics.AddOrUpdate("last_send_time", DateTime.UtcNow, (k, v) => DateTime.UtcNow);
        }

        public void RecordBatch(int messageCount, bool success, TimeSpan duration)
        {
            Interlocked.Increment(ref _totalBatches);
            Interlocked.Add(ref _totalSentMessages, messageCount);

            if (!success)
            {
                Interlocked.Increment(ref _totalErrors);
            }

            _metrics.AddOrUpdate("last_batch_size", messageCount, (k, v) => messageCount);
            _metrics.AddOrUpdate("last_batch_duration", duration, (k, v) => duration);
            _metrics.AddOrUpdate("last_batch_success", success, (k, v) => success);
            _metrics.AddOrUpdate("last_batch_time", DateTime.UtcNow, (k, v) => DateTime.UtcNow);
        }

        public void RecordSubscription<T>(string subscriptionId, bool success) where T : class
        {
            if (success)
            {
                Interlocked.Increment(ref _activeSubscriptions);
            }

            _metrics.AddOrUpdate($"subscription_{typeof(T).Name}", subscriptionId, (k, v) => subscriptionId);
            _metrics.AddOrUpdate("last_subscription_time", DateTime.UtcNow, (k, v) => DateTime.UtcNow);
        }

        public void RecordUnsubscribe(string subscriptionId, bool success)
        {
            if (success)
            {
                Interlocked.Decrement(ref _activeSubscriptions);
            }

            _metrics.AddOrUpdate("last_unsubscribe_time", DateTime.UtcNow, (k, v) => DateTime.UtcNow);
            _metrics.AddOrUpdate("last_unsubscribe_success", success, (k, v) => success);
        }

        public Dictionary<string, object> GetMetricsSnapshot()
        {
            var snapshot = new Dictionary<string, object>
            {
                ["total_sent_messages"] = _totalSentMessages,
                ["total_batches"] = _totalBatches,
                ["total_errors"] = _totalErrors,
                ["active_subscriptions"] = _activeSubscriptions,
                ["uptime"] = DateTime.UtcNow - StartTime,
                ["error_rate"] = _totalSentMessages > 0 ? (double)_totalErrors / _totalSentMessages : 0.0
            };

            foreach (var kvp in _metrics)
            {
                snapshot[kvp.Key] = kvp.Value;
            }

            return snapshot;
        }

        public void Reset()
        {
            Interlocked.Exchange(ref _totalSentMessages, 0);
            Interlocked.Exchange(ref _totalBatches, 0);
            Interlocked.Exchange(ref _totalErrors, 0);
            Interlocked.Exchange(ref _activeSubscriptions, 0);
            _metrics.Clear();
        }
    }

    public static class PerformanceStatsExtensions
    {
        public static PoolStatistics ConvertToPoolStats(this ProducerPerformanceStats stats)
        {
            return new PoolStatistics
            {
                TotalPools = 1,
                ActiveResources = stats.ActiveProducers,
                PooledResources = 0,
                TotalRentCount = stats.TotalMessages,
                TotalReturnCount = stats.SuccessfulMessages,
                TotalDiscardedCount = stats.FailedMessages,
                AverageUtilization = stats.ThroughputPerSecond > 0 ? 0.8 : 0.0,
                LastUpdated = stats.LastUpdated
            };
        }

        public static PoolStatistics ConvertToPoolStats(this ConsumerPerformanceStats stats)
        {
            return new PoolStatistics
            {
                TotalPools = 1,
                ActiveResources = stats.ActiveConsumers,
                PooledResources = 0,
                TotalRentCount = stats.TotalMessages,
                TotalReturnCount = stats.ProcessedMessages,
                TotalDiscardedCount = stats.FailedMessages,
                AverageUtilization = stats.ThroughputPerSecond > 0 ? 0.8 : 0.0,
                LastUpdated = stats.LastUpdated
            };
        }
    }
}