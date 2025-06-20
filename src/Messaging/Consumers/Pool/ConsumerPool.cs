using Confluent.Kafka;
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Messaging.Consumers.Core;
using KsqlDsl.Messaging.Consumers.Exceptions;
using KsqlDsl.Monitoring.Abstractions;
using KsqlDsl.Monitoring.Abstractions.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers.Pool;

public class ConsumerPool : IDisposable
{
    private readonly ConcurrentDictionary<ConsumerKey, ConcurrentQueue<PooledConsumer>> _pools = new();
    private readonly ConcurrentDictionary<ConsumerKey, ConsumerPoolMetrics> _poolMetrics = new();
    private readonly ConcurrentDictionary<string, ConsumerInstance> _activeConsumers = new(); // 🔧 string key に変更
    private readonly ConsumerPoolConfig _config;
    private readonly ILogger<ConsumerPool> _logger;
    private readonly Timer _maintenanceTimer;
    private readonly Timer _healthCheckTimer;
    private readonly Timer _rebalanceTimer;
    private bool _disposed = false;

    public int MinPoolSize => _config.MinPoolSize;
    public int MaxPoolSize => _config.MaxPoolSize;

    public ConsumerPool(
        IOptions<ConsumerPoolConfig> config,
        ILogger<ConsumerPool> logger)
    {
        _config = config?.Value ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _maintenanceTimer = new Timer(PerformMaintenance, null,
            _config.MaintenanceInterval, _config.MaintenanceInterval);

        _healthCheckTimer = new Timer(PerformHealthCheck, null,
            _config.HealthCheckInterval, _config.HealthCheckInterval);

        _rebalanceTimer = new Timer(MonitorRebalancing, null,
            TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

        _logger.LogInformation("ConsumerPool initialized: Min={MinSize}, Max={MaxSize}, IdleTimeout={IdleTimeout}",
            _config.MinPoolSize, _config.MaxPoolSize, _config.ConsumerIdleTimeout);
    }

    public ConsumerInstance RentConsumer(ConsumerKey key)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        var pool = _pools.GetOrAdd(key, _ => new ConcurrentQueue<PooledConsumer>());
        var metrics = _poolMetrics.GetOrAdd(key, _ => new ConsumerPoolMetrics { ConsumerKey = key });

        while (pool.TryDequeue(out var pooledConsumer))
        {
            if (IsConsumerHealthy(pooledConsumer))
            {
                pooledConsumer.LastUsed = DateTime.UtcNow;
                pooledConsumer.UsageCount++;

                var instance = new ConsumerInstance
                {
                    ConsumerKey = key,
                    PooledConsumer = pooledConsumer,
                    RentedAt = DateTime.UtcNow,
                    IsActive = true
                };

                var instanceKey = GenerateInstanceKey(key, instance);
                _activeConsumers[instanceKey] = instance;

                lock (metrics)
                {
                    metrics.RentCount++;
                    metrics.ActiveConsumers++;
                }

                _logger.LogTrace("Consumer rented from pool: {ConsumerKey} (Usage: {UsageCount})",
                    key, pooledConsumer.UsageCount);

                return instance;
            }
            else
            {
                DisposeConsumerSafely(pooledConsumer.Consumer);
                RecordConsumerDisposal(key, "unhealthy");
            }
        }

        var newConsumer = CreateNewConsumer(key);
        var newInstance = new ConsumerInstance
        {
            ConsumerKey = key,
            PooledConsumer = new PooledConsumer
            {
                Consumer = newConsumer,
                CreatedAt = DateTime.UtcNow,
                LastUsed = DateTime.UtcNow,
                UsageCount = 1,
                IsHealthy = true
            },
            RentedAt = DateTime.UtcNow,
            IsActive = true
        };

        var newInstanceKey = GenerateInstanceKey(key, newInstance);
        _activeConsumers[newInstanceKey] = newInstance;

        lock (metrics)
        {
            metrics.CreatedCount++;
            metrics.ActiveConsumers++;
        }

        _logger.LogDebug("New consumer created for key: {ConsumerKey} (Total created: {CreatedCount})",
            key, metrics.CreatedCount);

        return newInstance;
    }

    public void ReturnConsumer(ConsumerKey key, ConsumerInstance instance)
    {
        if (key == null || instance == null) return;

        try
        {
            var instanceKey = GenerateInstanceKey(key, instance);
            _activeConsumers.TryRemove(instanceKey, out _);

            var pool = _pools.GetOrAdd(key, _ => new ConcurrentQueue<PooledConsumer>());
            var metrics = _poolMetrics.GetOrAdd(key, _ => new ConsumerPoolMetrics { ConsumerKey = key });

            var currentPoolSize = pool.Count;

            if (!IsConsumerHealthy(instance.PooledConsumer))
            {
                DisposeConsumerSafely(instance.PooledConsumer.Consumer);
                RecordConsumerDisposal(key, "unhealthy_on_return");

                lock (metrics)
                {
                    metrics.ActiveConsumers--;
                    metrics.DiscardedCount++;
                }
                return;
            }

            if (currentPoolSize >= _config.MaxPoolSize)
            {
                DisposeConsumerSafely(instance.PooledConsumer.Consumer);
                RecordConsumerDisposal(key, "pool_full");

                lock (metrics)
                {
                    metrics.ActiveConsumers--;
                    metrics.DiscardedCount++;
                }

                _logger.LogTrace("Consumer discarded due to pool limit: {ConsumerKey} (Pool size: {PoolSize})",
                    key, currentPoolSize);
                return;
            }

            instance.PooledConsumer.LastUsed = DateTime.UtcNow;
            instance.IsActive = false;
            pool.Enqueue(instance.PooledConsumer);

            lock (metrics)
            {
                metrics.ReturnCount++;
                metrics.ActiveConsumers--;
            }

            _logger.LogTrace("Consumer returned to pool: {ConsumerKey} (Pool size: {PoolSize})",
                key, currentPoolSize + 1);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to return consumer to pool: {ConsumerKey}", key);
            DisposeConsumerSafely(instance.PooledConsumer?.Consumer);
        }
    }

    public async Task<List<TopicPartition>> GetAssignedPartitionsAsync(ConsumerKey key)
    {
        var assignedPartitions = new List<TopicPartition>();

        var activeInstances = _activeConsumers.Values
            .Where(i => i.ConsumerKey.Equals(key) && i.IsActive)
            .ToList();

        foreach (var instance in activeInstances)
        {
            try
            {
                if (instance.PooledConsumer?.Consumer != null)
                {
                    await Task.Delay(1);
                    var partitions = instance.PooledConsumer.Consumer.Assignment;
                    if (partitions != null)
                    {
                        assignedPartitions.AddRange(partitions);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get assigned partitions for consumer: {ConsumerKey}", key);
            }
        }

        return assignedPartitions.Distinct().ToList();
    }

    public async Task<Dictionary<TopicPartition, Offset>> GetOffsetsAsync(ConsumerKey key)
    {
        var allOffsets = new Dictionary<TopicPartition, Offset>();

        var activeInstances = _activeConsumers.Values
            .Where(i => i.ConsumerKey.Equals(key) && i.IsActive)
            .ToList();

        foreach (var instance in activeInstances)
        {
            try
            {
                if (instance.PooledConsumer?.Consumer != null)
                {
                    await Task.Delay(1);
                    var assignment = instance.PooledConsumer.Consumer.Assignment;
                    if (assignment != null)
                    {
                        foreach (var partition in assignment)
                        {
                            try
                            {
                                var committed = instance.PooledConsumer.Consumer.Committed(new[] { partition }, TimeSpan.FromSeconds(5));
                                if (committed?.FirstOrDefault()?.Offset != null)
                                {
                                    allOffsets[partition] = committed.First().Offset;
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogTrace(ex, "Failed to get offset for partition: {Partition}", partition);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get offsets for consumer: {ConsumerKey}", key);
            }
        }

        return allOffsets;
    }

    public async Task<ConsumerPoolHealthStatus> GetHealthStatusAsync()
    {
        await Task.Delay(1);

        try
        {
            var status = new ConsumerPoolHealthStatus
            {
                HealthLevel = ConsumerPoolHealthLevel.Healthy,
                TotalPools = _pools.Count,
                TotalActiveConsumers = GetActiveConsumerCount(),
                TotalPooledConsumers = GetTotalPooledConsumers(),
                PoolMetrics = GetAllPoolMetrics(),
                Issues = new List<ConsumerPoolHealthIssue>(),
                LastCheck = DateTime.UtcNow
            };

            var unhealthyPools = 0;
            var rebalanceIssues = 0;
            var overloadedPools = 0;

            foreach (var kvp in _poolMetrics)
            {
                var metrics = kvp.Value;
                var poolSize = _pools.TryGetValue(kvp.Key, out var pool) ? pool.Count : 0;

                lock (metrics)
                {
                    if (metrics.FailureRate > 0.1)
                    {
                        unhealthyPools++;
                    }

                    if (metrics.RebalanceFailures > 0)
                    {
                        rebalanceIssues++;
                    }

                    if (poolSize == 0 && metrics.ActiveConsumers > _config.MaxPoolSize * 0.8)
                    {
                        overloadedPools++;
                    }
                }
            }

            if (unhealthyPools > _pools.Count * 0.2)
            {
                status.HealthLevel = ConsumerPoolHealthLevel.Critical;
                status.Issues.Add(new ConsumerPoolHealthIssue
                {
                    Type = ConsumerPoolHealthIssueType.HighFailureRate,
                    Description = $"{unhealthyPools} pools have high failure rates",
                    Severity = ConsumerPoolIssueSeverity.Critical
                });
            }
            else if (rebalanceIssues > 0)
            {
                status.HealthLevel = ConsumerPoolHealthLevel.Warning;
                status.Issues.Add(new ConsumerPoolHealthIssue
                {
                    Type = ConsumerPoolHealthIssueType.RebalanceFailure,
                    Description = $"{rebalanceIssues} pools have rebalance issues",
                    Severity = ConsumerPoolIssueSeverity.Warning
                });
            }
            else if (overloadedPools > 0)
            {
                status.HealthLevel = ConsumerPoolHealthLevel.Warning;
                status.Issues.Add(new ConsumerPoolHealthIssue
                {
                    Type = ConsumerPoolHealthIssueType.PoolExhaustion,
                    Description = $"{overloadedPools} pools are experiencing high load",
                    Severity = ConsumerPoolIssueSeverity.Warning
                });
            }

            return status;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get consumer pool health status");
            return new ConsumerPoolHealthStatus
            {
                HealthLevel = ConsumerPoolHealthLevel.Critical,
                Issues = new List<ConsumerPoolHealthIssue>
                {
                    new() {
                        Type = ConsumerPoolHealthIssueType.HealthCheckFailure,
                        Description = $"Health check failed: {ex.Message}",
                        Severity = ConsumerPoolIssueSeverity.Critical
                    }
                },
                LastCheck = DateTime.UtcNow
            };
        }
    }

    public void RebalanceConsumers()
    {
        var rebalanceCount = 0;

        try
        {
            var consumersByGroup = _activeConsumers.Values
                .Where(i => i.IsActive)
                .GroupBy(i => i.ConsumerKey.GroupId)
                .ToList();

            foreach (var group in consumersByGroup)
            {
                var consumers = group.ToList();

                if (consumers.Count > 1)
                {
                    var overloadedConsumers = consumers
                        .Where(c => c.PooledConsumer?.UsageCount > 100)
                        .ToList();

                    var underloadedConsumers = consumers
                        .Where(c => c.PooledConsumer?.UsageCount < 10)
                        .ToList();

                    if (overloadedConsumers.Any() && underloadedConsumers.Any())
                    {
                        _logger.LogInformation("Rebalancing consumers in group {GroupId}: {OverloadedCount} overloaded, {UnderloadedCount} underloaded",
                            group.Key, overloadedConsumers.Count, underloadedConsumers.Count);

                        rebalanceCount++;
                    }
                }
            }

            if (rebalanceCount > 0)
            {
                _logger.LogInformation("Consumer rebalancing completed: {RebalanceCount} groups rebalanced", rebalanceCount);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during consumer rebalancing");
        }
    }

    public ConsumerPoolDiagnostics GetDiagnostics()
    {
        return new ConsumerPoolDiagnostics
        {
            Configuration = _config,
            TotalPools = _pools.Count,
            TotalActiveConsumers = GetActiveConsumerCount(),
            TotalPooledConsumers = GetTotalPooledConsumers(),
            PoolMetrics = GetAllPoolMetrics(),
            SystemMetrics = new Dictionary<string, object>
            {
                ["MemoryUsage"] = GC.GetTotalMemory(false),
                ["ThreadCount"] = System.Diagnostics.Process.GetCurrentProcess().Threads.Count,
                ["ActiveConsumerInstances"] = _activeConsumers.Count
            }
        };
    }

    public int GetActiveConsumerCount()
    {
        return _activeConsumers.Values.Count(i => i.IsActive);
    }

    private int GetTotalPooledConsumers()
    {
        return _pools.Values.Sum(pool => pool.Count);
    }

    private Dictionary<ConsumerKey, PoolMetrics> GetAllPoolMetrics()
    {
        var result = new Dictionary<ConsumerKey, PoolMetrics>();

        foreach (var kvp in _poolMetrics)
        {
            var consumerMetrics = kvp.Value;
            result[kvp.Key] = new PoolMetrics
            {
                ConsumerKey = kvp.Key,
                CreatedCount = consumerMetrics.CreatedCount,
                CreationFailures = consumerMetrics.CreationFailures,
                RentCount = consumerMetrics.RentCount,
                ReturnCount = consumerMetrics.ReturnCount,
                DiscardedCount = consumerMetrics.DiscardedCount,
                DisposedCount = consumerMetrics.DisposedCount,
                ActiveConsumers = consumerMetrics.ActiveConsumers
            };
        }

        return result;
    }

    // ✅ 修正: 文字列キーを返すように変更
    private string GenerateInstanceKey(ConsumerKey key, ConsumerInstance instance)
    {
        return $"{key}:{instance.RentedAt.Ticks}";
    }

    private IConsumer<object, object> CreateNewConsumer(ConsumerKey key)
    {
        try
        {
            var config = BuildConsumerConfig(key);
            var consumer = new ConsumerBuilder<object, object>(config).Build();

            _logger.LogTrace("New consumer created for {ConsumerKey}", key);
            return consumer;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create consumer for {ConsumerKey}", key);

            var metrics = _poolMetrics.GetOrAdd(key, _ => new ConsumerPoolMetrics { ConsumerKey = key });
            lock (metrics)
            {
                metrics.CreationFailures++;
            }

            throw new ConsumerPoolException($"Failed to create consumer for key: {key}", ex);
        }
    }

    private ConsumerConfig BuildConsumerConfig(ConsumerKey key)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = key.GroupId,
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Latest,
            EnableAutoCommit = true,
            SessionTimeoutMs = 30000,
            HeartbeatIntervalMs = 3000,
            MaxPollIntervalMs = 300000,
            FetchMinBytes = 1,
            FetchMaxBytes = 52428800
        };

        return config;
    }

    private bool IsConsumerHealthy(PooledConsumer pooledConsumer)
    {
        if (pooledConsumer?.Consumer == null) return false;

        try
        {
            var handle = pooledConsumer.Consumer.Handle;
            if (handle == null ) return false;

            var idleTime = DateTime.UtcNow - pooledConsumer.LastUsed;
            if (idleTime > _config.ConsumerIdleTimeout) return false;

            return pooledConsumer.IsHealthy;
        }
        catch
        {
            return false;
        }
    }

    private void DisposeConsumerSafely(IConsumer<object, object>? consumer)
    {
        try
        {
            consumer?.Close();
            consumer?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing consumer");
        }
    }

    private void RecordConsumerDisposal(ConsumerKey key, string reason)
    {
        var metrics = _poolMetrics.GetOrAdd(key, _ => new ConsumerPoolMetrics { ConsumerKey = key });

        lock (metrics)
        {
            metrics.DisposedCount++;
            metrics.LastDisposalReason = reason;
            metrics.LastDisposalTime = DateTime.UtcNow;
        }

        _logger.LogTrace("Consumer disposed: {ConsumerKey}, Reason: {Reason}", key, reason);
    }

    private void PerformMaintenance(object? state)
    {
        try
        {
            TrimExcessConsumers();
            OptimizePoolSizes();

            _logger.LogTrace("Consumer pool maintenance completed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during consumer pool maintenance");
        }
    }

    private void PerformHealthCheck(object? state)
    {
        try
        {
            var _ = GetHealthStatusAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during consumer pool health check");
        }
    }

    private void MonitorRebalancing(object? state)
    {
        if (_config.EnableRebalanceOptimization)
        {
            try
            {
                RebalanceConsumers();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during rebalance monitoring");
            }
        }
    }

    private void TrimExcessConsumers()
    {
        var trimCount = 0;
        var now = DateTime.UtcNow;

        foreach (var kvp in _pools)
        {
            var key = kvp.Key;
            var pool = kvp.Value;
            var tempQueue = new ConcurrentQueue<PooledConsumer>();

            while (pool.TryDequeue(out var pooledConsumer))
            {
                var idleTime = now - pooledConsumer.LastUsed;

                if (idleTime <= _config.ConsumerIdleTimeout &&
                    IsConsumerHealthy(pooledConsumer) &&
                    tempQueue.Count < _config.MaxPoolSize)
                {
                    tempQueue.Enqueue(pooledConsumer);
                }
                else
                {
                    DisposeConsumerSafely(pooledConsumer.Consumer);
                    trimCount++;
                    RecordConsumerDisposal(key, idleTime > _config.ConsumerIdleTimeout ? "idle_timeout" : "unhealthy");
                }
            }

            while (tempQueue.TryDequeue(out var survivingConsumer))
            {
                pool.Enqueue(survivingConsumer);
            }
        }

        if (trimCount > 0)
        {
            _logger.LogInformation("Trimmed {TrimCount} excess consumers from pools", trimCount);
        }
    }

    private void OptimizePoolSizes()
    {
        foreach (var kvp in _pools)
        {
            var key = kvp.Key;
            var pool = kvp.Value;
            var metrics = _poolMetrics.GetOrAdd(key, _ => new ConsumerPoolMetrics { ConsumerKey = key });

            lock (metrics)
            {
                var utilizationRate = metrics.ActiveConsumers > 0 ?
                    (double)metrics.RentCount / (metrics.RentCount + pool.Count) : 0;

                if (utilizationRate < 0.1 && pool.Count > _config.MinPoolSize)
                {
                    var targetSize = Math.Max(_config.MinPoolSize, pool.Count / 2);
                    var removeCount = pool.Count - targetSize;

                    for (int i = 0; i < removeCount && pool.TryDequeue(out var consumer); i++)
                    {
                        DisposeConsumerSafely(consumer.Consumer);
                        RecordConsumerDisposal(key, "pool_optimization");
                    }
                }
            }
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _logger.LogInformation("Disposing ConsumerPool...");

            _maintenanceTimer?.Dispose();
            _healthCheckTimer?.Dispose();
            _rebalanceTimer?.Dispose();

            foreach (var instance in _activeConsumers.Values)
            {
                DisposeConsumerSafely(instance.PooledConsumer?.Consumer);
            }
            _activeConsumers.Clear();

            var totalDisposed = 0;
            foreach (var pool in _pools.Values)
            {
                while (pool.TryDequeue(out var pooledConsumer))
                {
                    DisposeConsumerSafely(pooledConsumer.Consumer);
                    totalDisposed++;
                }
            }

            _pools.Clear();
            _poolMetrics.Clear();

            _disposed = true;
            _logger.LogInformation("ConsumerPool disposed: {TotalDisposed} consumers disposed", totalDisposed);
        }
    }
}