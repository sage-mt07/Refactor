using System;
using System.Collections.Generic;

namespace KsqlDsl.Messaging.Bus;

public class BusDiagnostics
{
    public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;

    public Dictionary<string, object> PerformanceMetrics { get; set; } = new();
    public Dictionary<string, object> SystemInfo { get; set; } = new();

    public string GetSummary()
    {
        return $@"Message Bus Diagnostics (Generated: {GeneratedAt:yyyy-MM-dd HH:mm:ss})
Health Level: {HealthStatus.HealthLevel}
Active Subscriptions: {SubscriptionStats.ActiveSubscriptions}
Producer Pool: {ProducerPoolStats.ActiveResources} active, {ProducerPoolStats.PooledResources} pooled
Consumer Pool: {ConsumerPoolStats.ActiveResources} active, {ConsumerPoolStats.PooledResources} pooled
Issues: {HealthStatus.Issues.Count}";
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

   
}