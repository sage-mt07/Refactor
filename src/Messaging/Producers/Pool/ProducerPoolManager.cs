using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KsqlDsl.Messaging.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using KsqlDsl.Monitoring.Metrics;
using KsqlDsl.Core.Models;
using KsqlDsl.Messaging.Producers.Core;
using KsqlDsl.Messaging.Producers.Exception;

namespace KsqlDsl.Messaging.Producers.Pool
{
    public class ProducerPoolManager : IPoolManager<ProducerKey, IProducer<object, object>>
    {
        private readonly ConcurrentDictionary<ProducerKey, ConcurrentQueue<PooledProducer>> _pools = new();
        private readonly ConcurrentDictionary<ProducerKey, PoolMetrics> _poolMetrics = new();
        private readonly ProducerPoolConfig _config;
        private readonly ILogger<ProducerPoolManager> _logger;
        private readonly Timer _maintenanceTimer;
        private readonly Timer _healthCheckTimer;
        private bool _disposed = false;

        public ProducerPoolManager(
            IOptions<ProducerPoolConfig> config,
            ILogger<ProducerPoolManager> logger)
        {
            _config = config?.Value ?? throw new ArgumentNullException(nameof(config));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            _maintenanceTimer = new Timer(PerformMaintenance, null,
                _config.MaintenanceInterval, _config.MaintenanceInterval);

            _healthCheckTimer = new Timer(PerformHealthCheck, null,
                _config.HealthCheckInterval, _config.HealthCheckInterval);
        }

        public IProducer<object, object> RentResource(ProducerKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            var pool = _pools.GetOrAdd(key, _ => new ConcurrentQueue<PooledProducer>());
            var metrics = _poolMetrics.GetOrAdd(key, _ => new PoolMetrics { ProducerKey = key });

            while (pool.TryDequeue(out var pooledProducer))
            {
                if (IsProducerHealthy(pooledProducer))
                {
                    pooledProducer.LastUsed = DateTime.UtcNow;
                    pooledProducer.UsageCount++;

                    lock (metrics)
                    {
                        metrics.RentCount++;
                        metrics.ActiveProducers++;
                    }

                    return pooledProducer.Producer;
                }
                else
                {
                    DisposeProducerSafely(pooledProducer.Producer);
                    RecordProducerDisposal(key, "unhealthy");
                }
            }

            var newProducer = CreateNewProducer(key);

            lock (metrics)
            {
                metrics.CreatedCount++;
                metrics.ActiveProducers++;
            }

            return newProducer;
        }

        public void ReturnResource(ProducerKey key, IProducer<object, object> producer)
        {
            if (key == null || producer == null) return;

            try
            {
                var pool = _pools.GetOrAdd(key, _ => new ConcurrentQueue<PooledProducer>());
                var metrics = _poolMetrics.GetOrAdd(key, _ => new PoolMetrics { ProducerKey = key });

                var currentPoolSize = pool.Count;

                if (currentPoolSize >= _config.MaxPoolSize)
                {
                    DisposeProducerSafely(producer);
                    RecordProducerDisposal(key, "pool_full");

                    lock (metrics)
                    {
                        metrics.ActiveProducers--;
                        metrics.DiscardedCount++;
                    }
                    return;
                }

                var pooledProducer = new PooledProducer
                {
                    Producer = producer,
                    CreatedAt = DateTime.UtcNow,
                    LastUsed = DateTime.UtcNow,
                    UsageCount = 0
                };

                pool.Enqueue(pooledProducer);

                lock (metrics)
                {
                    metrics.ReturnCount++;
                    metrics.ActiveProducers--;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to return producer to pool: {ProducerKey}", key);
                DisposeProducerSafely(producer);
            }
        }

        public PoolStatistics GetStatistics()
        {
            return new PoolStatistics
            {
                TotalPools = _pools.Count,
                ActiveResources = _poolMetrics.Values.Sum(m => m.ActiveProducers),
                PooledResources = _pools.Values.Sum(pool => pool.Count),
                TotalRentCount = _poolMetrics.Values.Sum(m => m.RentCount),
                TotalReturnCount = _poolMetrics.Values.Sum(m => m.ReturnCount),
                TotalDiscardedCount = _poolMetrics.Values.Sum(m => m.DiscardedCount),
                AverageUtilization = CalculateAverageUtilization(),
                LastUpdated = DateTime.UtcNow
            };
        }

        public async Task<KsqlDsl.Core.Abstractions.PoolHealthStatus> GetHealthStatusAsync()
        {
            await Task.Delay(1);

            try
            {
                var status = new PoolHealthStatus
                {
                    HealthLevel = KsqlDsl.Messaging.Abstractions.PoolHealthLevel.Healthy,
                    Statistics = GetStatistics(),
                    Issues = new List<KsqlDsl.Core.Abstractions.PoolHealthIssue>(),
                    LastCheck = DateTime.UtcNow
                };

                var unhealthyPools = 0;
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

                        if (poolSize == 0 && metrics.ActiveProducers > _config.MaxPoolSize * 0.8)
                        {
                            overloadedPools++;
                        }
                    }
                }

                if (unhealthyPools > _pools.Count * 0.2)
                {
                    status.HealthLevel = KsqlDsl.Messaging.Abstractions.PoolHealthLevel.Critical;
                    status.Issues.Add(new PoolHealthIssue
                    {
                        Type = KsqlDsl.Messaging.Abstractions.PoolHealthIssueType.HighFailureRate,
                        Description = $"{unhealthyPools} pools have high failure rates",
                        Severity = KsqlDsl.Messaging.Abstractions.PoolIssueSeverity.Critical
                    });
                }
                else if (overloadedPools > 0)
                {
                    status.HealthLevel = KsqlDsl.Messaging.Abstractions.PoolHealthLevel.Warning;
                    status.Issues.Add(new PoolHealthIssue
                    {
                        Type = KsqlDsl.Messaging.Abstractions.PoolHealthIssueType.PoolExhaustion,
                        Description = $"{overloadedPools} pools are experiencing high load",
                        Severity = KsqlDsl.Messaging.Abstractions.PoolIssueSeverity.Warning
                    });
                }

                return status;
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Failed to get pool health status");
                return new PoolHealthStatus
                {
                    HealthLevel = KsqlDsl.Messaging.Abstractions.PoolHealthLevel.Critical,
                    Issues = new List<PoolHealthIssue>
                    {
                        new() {
                            Type = KsqlDsl.Messaging.Abstractions.PoolHealthIssueType.HealthCheckFailure,
                            Description = $"Health check failed: {ex.Message}",
                            Severity = KsqlDsl.Messaging.Abstractions.PoolIssueSeverity.Critical
                        }
                    },
                    LastCheck = DateTime.UtcNow
                };
            }
        }

        public int GetActiveResourceCount()
        {
            return _poolMetrics.Values.Sum(m => m.ActiveProducers);
        }

        public void OptimizePools()
        {
            var trimCount = 0;
            var now = DateTime.UtcNow;

            foreach (var kvp in _pools)
            {
                var key = kvp.Key;
                var pool = kvp.Value;
                var tempQueue = new ConcurrentQueue<PooledProducer>();

                while (pool.TryDequeue(out var pooledProducer))
                {
                    var idleTime = now - pooledProducer.LastUsed;

                    if (idleTime <= _config.ProducerIdleTimeout &&
                        IsProducerHealthy(pooledProducer) &&
                        tempQueue.Count < _config.MaxPoolSize)
                    {
                        tempQueue.Enqueue(pooledProducer);
                    }
                    else
                    {
                        DisposeProducerSafely(pooledProducer.Producer);
                        trimCount++;
                        RecordProducerDisposal(key, idleTime > _config.ProducerIdleTimeout ? "idle_timeout" : "unhealthy");
                    }
                }

                while (tempQueue.TryDequeue(out var survivingProducer))
                {
                    pool.Enqueue(survivingProducer);
                }
            }

            if (trimCount > 0)
            {
                _logger.LogInformation("Optimized pools: trimmed {TrimCount} excess producers", trimCount);
            }
        }

        private IProducer<object, object> CreateNewProducer(ProducerKey key)
        {
            try
            {
                var config = BuildProducerConfig(key);
                var producer = new ProducerBuilder<object, object>(config).Build();
                return producer;
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Failed to create producer for {ProducerKey}", key);

                var metrics = _poolMetrics.GetOrAdd(key, _ => new PoolMetrics { ProducerKey = key });
                lock (metrics)
                {
                    metrics.CreationFailures++;
                }

                throw new ProducerPoolException($"Failed to create producer for key: {key}", ex);
            }
        }

        private ProducerConfig BuildProducerConfig(ProducerKey key)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.All,
                EnableIdempotence = true,
                MaxInFlight = 1,
                CompressionType = CompressionType.Snappy,
                LingerMs = 5,
                BatchSize = 16384
            };

            return config;
        }

        private bool IsProducerHealthy(PooledProducer pooledProducer)
        {
            if (pooledProducer?.Producer == null) return false;

            try
            {
                var handle = pooledProducer.Producer.Handle;
                return handle != null ;
            }
            catch
            {
                return false;
            }
        }

        private void DisposeProducerSafely(IProducer<object, object> producer)
        {
            try
            {
                producer?.Dispose();
            }
            catch (System.Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing producer");
            }
        }

        private void RecordProducerDisposal(ProducerKey key, string reason)
        {
            var metrics = _poolMetrics.GetOrAdd(key, _ => new PoolMetrics { ProducerKey = key });

            lock (metrics)
            {
                metrics.DisposedCount++;
                metrics.LastDisposalReason = reason;
                metrics.LastDisposalTime = DateTime.UtcNow;
            }
        }

        private double CalculateAverageUtilization()
        {
            if (_poolMetrics.IsEmpty) return 0.0;

            var totalUtilization = 0.0;
            var poolCount = 0;

            foreach (var metrics in _poolMetrics.Values)
            {
                lock (metrics)
                {
                    if (metrics.RentCount > 0)
                    {
                        totalUtilization += (double)metrics.ActiveProducers / _config.MaxPoolSize;
                        poolCount++;
                    }
                }
            }

            return poolCount > 0 ? totalUtilization / poolCount : 0.0;
        }

        private void PerformMaintenance(object? state)
        {
            try
            {
                OptimizePools();
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Error during pool maintenance");
            }
        }

        private void PerformHealthCheck(object? state)
        {
            try
            {
                var _ = GetHealthStatusAsync().GetAwaiter().GetResult();
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Error during pool health check");
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _maintenanceTimer?.Dispose();
                _healthCheckTimer?.Dispose();

                var totalDisposed = 0;
                foreach (var pool in _pools.Values)
                {
                    while (pool.TryDequeue(out var pooledProducer))
                    {
                        DisposeProducerSafely(pooledProducer.Producer);
                        totalDisposed++;
                    }
                }

                _pools.Clear();
                _poolMetrics.Clear();

                _disposed = true;
                _logger.LogInformation("ProducerPoolManager disposed: {TotalDisposed} producers disposed", totalDisposed);
            }
        }
    }

    public interface IProducerMetricsCollector
    {
        void RecordSend(bool success, TimeSpan duration, int messageSize);
        void RecordBatch(int messageCount, bool success, TimeSpan duration, int totalBytes);
    }
}