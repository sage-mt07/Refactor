using Confluent.Kafka;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Monitoring.Metrics;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Cache
{
    public class AvroSerializerCache : ISerializationManager<object>, IDisposable
    {
        private readonly ConcurrentDictionary<Type, object> _serializerManagers = new();
        private readonly AvroSerializerFactory _factory;
        private readonly ILogger<AvroSerializerCache>? _logger;
        private readonly SerializationStatistics _statistics = new();
        private bool _disposed = false;
        private readonly Dictionary<string, object> _deserializerCache = new();
        private readonly Dictionary<string, object> _serializerCache = new();

        public Type EntityType => typeof(object);

        public AvroSerializerCache(
            AvroSerializerFactory factory,
            ILoggerFactory? logger = null)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _logger = logger?.CreateLogger<AvroSerializerCache>()
                ?? NullLogger<AvroSerializerCache>.Instance;
        }
        private void ClearSerializerCache()
        {
            _serializerCache.Clear();
            _logger?.LogDebug("Serializer cache cleared");
        }
        private void ClearDeserializerCache()
        {
            _deserializerCache.Clear();
            _logger?.LogDebug("Deserializer cache cleared");
        }
        public IAvroSerializationManager<T> GetAvroManager<T>() where T : class
        {
            var entityType = typeof(T);

            if (_serializerManagers.TryGetValue(entityType, out var existingManager))
            {
                return (IAvroSerializationManager<T>)existingManager;
            }

            var newManager = new AvroEntitySerializationManager<T>(_factory, _logger);
            _serializerManagers[entityType] = newManager;
            return newManager;
        }

        public virtual IAvroSerializer<T> GetOrCreateSerializer<T>() where T : class
        {
            var key = GetSerializerCacheKey<T>();
            if (_serializerCache.TryGetValue(key, out var cached))
            {
                return (IAvroSerializer<T>)cached;
            }

            var serializer = _factory.CreateSerializer<T>();
            _serializerCache[key] = serializer;

            _logger?.LogDebug("Created serializer for type {Type}", typeof(T).Name);

            return serializer;
        }
        public CoreSerializationStatistics GetAvroStatistics()
        {
            var aggregated = new CoreSerializationStatistics();

            // 各マネージャーの統計を集約
            foreach (var manager in _serializerManagers.Values)
            {
                if (manager is IAvroSerializationManager<object> typedManager)
                {
                    var stats = typedManager.GetStatistics();

                    // 統計情報を集約
                    aggregated.TotalOperations += stats.TotalSerializations + stats.TotalDeserializations;
                    aggregated.SuccessfulOperations += stats.TotalSerializations;
                    aggregated.FailedOperations += stats.CacheMisses;

                    // 重み付き平均レイテンシの計算
                    UpdateWeightedAverageLatency(aggregated, stats);
                }
            }

            aggregated.LastUpdated = DateTime.UtcNow;
            return aggregated;
        }
        private void UpdateWeightedAverageLatency(CoreSerializationStatistics aggregated, SerializationStatistics newStats)
        {
            var newOperations = newStats.TotalSerializations + newStats.TotalDeserializations;
            var previousOperations = aggregated.TotalOperations;
            var totalOperations = previousOperations + newOperations;

            if (totalOperations > 0)
            {
                var totalLatencyTicks = (aggregated.AverageLatency.Ticks * previousOperations) +
                                       (newStats.AverageLatency.Ticks * newOperations);
                aggregated.AverageLatency = new TimeSpan(totalLatencyTicks / totalOperations);
            }
        }

        private string GetSerializerCacheKey<T>()
        {
            return $"serializer:{typeof(T).FullName}";
        }

        public virtual IAvroDeserializer<T> GetOrCreateDeserializer<T>() where T : class
        {
            var key = GetDeserializerCacheKey<T>();

            if (_deserializerCache.TryGetValue(key, out var cached))
            {
                return (IAvroDeserializer<T>)cached;
            }

            var deserializer = _factory.CreateDeserializer<T>();
            _deserializerCache[key] = deserializer;

            _logger?.LogDebug("Created deserializer for type {Type}", typeof(T).Name);

            return deserializer;
        }

        private string GetDeserializerCacheKey<T>()
        {
            return $"deserializer:{typeof(T).FullName}";
        }
        public async Task<SerializerConfiguration<object>> GetConfigurationAsync()
        {
            // オブジェクト型の場合は汎用的な設定を返す
            await Task.CompletedTask;

            return new SerializerConfiguration<object>
            {
                KeySerializer = new object(), // プレースホルダー
                ValueSerializer = new object(), // プレースホルダー
                KeySchemaId = 0,
                ValueSchemaId = 0,
                CreatedAt = DateTime.UtcNow
            };
        }
        public Task<bool> ValidateAsync(object entity)
        {
            throw new NotSupportedException(
                "ValidateAsync for object type is not supported. " +
                "Use GetAvroManager<T>().ValidateRoundTripAsync() for type-specific validation.");
        }
        public Task<SerializerPair<object>> GetSerializersAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Use GetAvroManager<T>() for type-specific serializers");
        }

        public Task<DeserializerPair<object>> GetDeserializersAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Use GetAvroManager<T>() for type-specific deserializers");
        }

        public Task<bool> ValidateRoundTripAsync(object entity, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Use GetAvroManager<T>() for type-specific validation");
        }
        public AvroStatisticsReport? GetAvroStatisticsReport()
        {
            if (_avroCache == null)
            {
                _logger?.LogWarning("Cannot generate Avro statistics report: cache not available");
                return null;
            }

            try
            {
                // ✅ 新しいメソッドを使用
                return _avroCache.GenerateStatisticsReport();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to get Avro statistics report");
                return null;
            }
        }
        public MetricsSummary GetMetricsSummary()
        {
            var summary = new MetricsSummary
            {
                GeneratedAt = DateTime.UtcNow,
                CacheAvailable = _avroCache != null
            };

            if (_avroCache != null)
            {
                try
                {
                    var cachedItemCount = _avroCache.GetCachedItemCount();
                    // ✅ 修正: GetStatistics() → GetCurrentStatistics()
                    var globalStats = _avroCache.GetCurrentStatistics();

                    summary.CachedItemCount = cachedItemCount;
                    summary.TotalOperations = globalStats.TotalOperations;
                    summary.SuccessfulOperations = globalStats.SuccessfulOperations;
                    summary.FailedOperations = globalStats.FailedOperations;
                    summary.SuccessRate = globalStats.SuccessRate;
                    summary.AverageLatency = globalStats.AverageLatency;
                    summary.LastUpdated = globalStats.LastUpdated;
                }
                catch (Exception ex)
                {
                    summary.ErrorMessage = ex.Message;
                    _logger?.LogWarning(ex, "Failed to generate metrics summary");
                }
            }

            return summary;
        }
        public CacheEfficiencyReport GenerateCacheEfficiencyReport()
        {
            if (_avroCache == null)
            {
                return new CacheEfficiencyReport
                {
                    GeneratedAt = DateTime.UtcNow,
                    OverallEfficiency = 0.0,
                    TotalCachedItems = 0,
                    Recommendations = new List<string> { "Avro cache is not available" }
                };
            }

            try
            {
                var cachedItemCount = _avroCache.GetCachedItemCount();
                // ✅ 修正: GetStatistics() → GetCurrentStatistics()
                var globalStats = _avroCache.GetCurrentStatistics();

                var report = new CacheEfficiencyReport
                {
                    GeneratedAt = DateTime.UtcNow,
                    OverallEfficiency = globalStats.SuccessRate,
                    TotalCachedItems = cachedItemCount,
                    OverallHitRate = globalStats.SuccessRate,
                    OverallMissRate = globalStats.FailureRate,
                    TotalRequests = globalStats.TotalOperations,
                    SuccessfulOperations = globalStats.SuccessfulOperations,
                    FailedOperations = globalStats.FailedOperations,
                    AverageLatency = globalStats.AverageLatency,
                    LastUpdated = globalStats.LastUpdated,
                    Recommendations = GenerateRecommendations(globalStats.SuccessRate, cachedItemCount, globalStats.TotalOperations)
                };

                return report;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to generate cache efficiency report");
                return new CacheEfficiencyReport
                {
                    GeneratedAt = DateTime.UtcNow,
                    OverallEfficiency = 0.0,
                    TotalCachedItems = 0,
                    Recommendations = new List<string> { $"Report generation failed: {ex.Message}" }
                };
            }
        }
        private double GetCacheMissRate()
        {
            try
            {
                if (_avroCache == null) return 0.0;
                // ✅ 修正: 適切なメソッドを使用
                var stats = _avroCache.GetCurrentStatistics();
                return stats.FailureRate;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to get cache miss rate");
                return 0.0;
            }
        }

        private double GetCacheHitRate()
        {
            try
            {
                if (_avroCache == null) return 0.0;
                // ✅ 修正: 適切なメソッドを使用
                var stats = _avroCache.GetCurrentStatistics();
                return stats.SuccessRate;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to get cache hit rate");
                return 0.0;
            }
        }
        public void CollectCacheStatistics()
        {
            if (_avroCache == null)
            {
                if (_enableDebugLogging)
                {
                    _logger?.LogDebug("Avro cache is not available for statistics collection");
                }
                return;
            }

            try
            {
                var cachedItemCount = _avroCache.GetCachedItemCount();
                // ✅ 修正: GetStatistics() → GetCurrentStatistics() または GetAvroStatistics()
                var globalStats = _avroCache.GetCurrentStatistics(); // 基底クラスとの競合を避ける

                // ObservableGaugeは自動で値を取得するため、ここでは統計ログのみ
                if (_enableDebugLogging)
                {
                    _logger?.LogDebug(
                        "Cache statistics: Items={CachedItems}, SuccessRate={SuccessRate:P2}, Operations={TotalOps}",
                        cachedItemCount, globalStats.SuccessRate, globalStats.TotalOperations);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to collect cache statistics");
            }
        }
        public new CoreSerializationStatistics GetStatistics()
        {
            // 基底クラスのGetStatistics()を呼び出し、必要に応じて拡張
            var baseStats = base.GetStatistics();

            // 追加の統計情報があれば設定
            baseStats.LastUpdated = DateTime.UtcNow;

            return baseStats;
        }
        public CoreSerializationStatistics GetGlobalStatistics()
        {
            return GetStatistics();
        }
        public CacheEfficiencyInfo GetCacheEfficiencyInfo()
        {
            var stats = GetStatistics();
            var itemCount = GetCachedItemCount();

            return new CacheEfficiencyInfo
            {
                TotalOperations = stats.TotalOperations,
                SuccessfulOperations = stats.SuccessfulOperations,
                FailedOperations = stats.FailedOperations,
                AverageLatency = stats.AverageLatency,
                LastUpdated = stats.LastUpdated,
                CachedItemCount = itemCount,
                SuccessRate = stats.SuccessRate, // ✅ SuccessRateを使用
                MemoryEstimate = EstimateMemoryUsage()
            };
        }

        public CachePerformanceSummary GetPerformanceSummary()
        {
            var stats = GetStatistics();
            var efficiencyInfo = GetCacheEfficiencyInfo();

            return new CachePerformanceSummary
            {
                Timestamp = DateTime.UtcNow,
                ItemCount = efficiencyInfo.CachedItemCount,
                HitRate = efficiencyInfo.HitRate,
                AverageLatency = efficiencyInfo.AverageLatency,
                MemoryUsage = efficiencyInfo.MemoryEstimate,
                Status = DeterminePerformanceStatus(efficiencyInfo),
                LastActivity = efficiencyInfo.LastUpdated
            };
        }
        public Dictionary<string, TypeSpecificStats> GetTypeSpecificStatistics()
        {
            var typeStats = new Dictionary<string, TypeSpecificStats>();

            foreach (var kvp in _serializerManagers)
            {
                var entityType = kvp.Key;
                var typeName = entityType.Name;

                if (kvp.Value is IAvroSerializationManager<object> manager)
                {
                    var stats = manager.GetStatistics();
                    typeStats[typeName] = new TypeSpecificStats
                    {
                        TypeName = typeName,
                        TotalSerializations = stats.TotalSerializations,
                        TotalDeserializations = stats.TotalDeserializations,
                        CacheHits = stats.CacheHits,
                        CacheMisses = stats.CacheMisses,
                        SuccessRate = stats.HitRate, // SerializationStatistics.HitRateを使用
                        AverageLatency = stats.AverageLatency,
                        LastAccess = stats.LastUpdated
                    };
                }
            }

            return typeStats;
        }
        private CoreSerializationStatistics? GetBaseStatistics()
        {
            try
            {
                return base.GetStatistics();
            }
            catch (NotImplementedException)
            {
                // 基底クラスでGetStatisticsが実装されていない場合
                return null;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to get base statistics");
                return null;
            }
        }
        private bool ValidateStatistics(CoreSerializationStatistics stats)
        {
            if (stats == null) return false;
            if (stats.TotalOperations < 0) return false;
            if (stats.SuccessfulOperations < 0) return false;
            if (stats.FailedOperations < 0) return false;
            if (stats.SuccessfulOperations + stats.FailedOperations > stats.TotalOperations) return false;

            return true;
        }
        public void ResetCacheMetrics()
        {
            // 各マネージャーをリセット
            foreach (var manager in _serializerManagers.Values)
            {
                if (manager is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }

            _serializerManagers.Clear();
            _serializerCache.Clear();
            _deserializerCache.Clear();

            // ✅ 修正: 統計情報のリセット（_statisticsフィールドが存在する場合）
            var baseStats = GetBaseStatistics(); // 基底クラスの統計を取得
            if (baseStats != null)
            {
                baseStats.Reset(); // CoreSerializationStatisticsのResetメソッドを使用
            }

            _logger?.LogInformation("Avro serializer cache metrics have been reset");
        }
        private string DeterminePerformanceStatus(CacheEfficiencyInfo info)
        {
            if (info.SuccessRate >= 0.95 && info.AverageLatency.TotalMilliseconds < 10)
                return "Excellent";
            else if (info.SuccessRate >= 0.8 && info.AverageLatency.TotalMilliseconds < 50)
                return "Good";
            else if (info.SuccessRate >= 0.6)
                return "Fair";
            else
                return "Poor";
        }
        private long EstimateMemoryUsage()
        {
            // 各マネージャーあたりの推定メモリ使用量
            const long estimatedManagerSize = 1024; // 1KB per manager
            const long estimatedCacheOverhead = 256; // 256B overhead

            return (_serializerManagers.Count * estimatedManagerSize) + estimatedCacheOverhead;
        }
        private int CleanupUnusedManagers()
        {
            var cleanedCount = 0;
            var managersToRemove = new List<Type>();

            foreach (var kvp in _serializerManagers)
            {
                if (kvp.Value is IAvroSerializationManager<object> manager)
                {
                    var stats = manager.GetStatistics();

                    // 30分以上使用されていないマネージャーを削除候補とする
                    if (DateTime.UtcNow - stats.LastUpdated > TimeSpan.FromMinutes(30) &&
                        stats.TotalSerializations == 0 && stats.TotalDeserializations == 0)
                    {
                        managersToRemove.Add(kvp.Key);
                    }
                }
            }

            foreach (var type in managersToRemove)
            {
                if (_serializerManagers.TryRemove(type, out var manager))
                {
                    if (manager is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                    cleanedCount++;
                }
            }

            return cleanedCount;
        }
        public CacheMaintenanceResult PerformMaintenance()
        {
            var result = new CacheMaintenanceResult
            {
                MaintenanceTime = DateTime.UtcNow,
                ActionsPerformed = new List<string>()
            };

            try
            {
                var initialUsage = GetCacheUsageDetails();

                // 使用されていないマネージャーのクリーンアップ
                var cleanedManagers = CleanupUnusedManagers();
                if (cleanedManagers > 0)
                {
                    result.ActionsPerformed.Add($"Cleaned up {cleanedManagers} unused managers");
                }

                // 統計情報の正規化
                var stats = GetValidatedStatistics();
                if (stats.TotalOperations == 0)
                {
                    // 統計をリセット
                    result.ActionsPerformed.Add("Reset empty statistics");
                }

                var finalUsage = GetCacheUsageDetails();
                result.MemoryFreed = Math.Max(0, initialUsage.EstimatedMemoryUsage - finalUsage.EstimatedMemoryUsage);
                result.Success = true;

                if (result.ActionsPerformed.Count == 0)
                {
                    result.ActionsPerformed.Add("No maintenance actions needed");
                }
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.ActionsPerformed.Add($"Maintenance failed: {ex.Message}");
                _logger?.LogError(ex, "Cache maintenance failed");
            }

            return result;
        }
        public CoreSerializationStatistics GetValidatedStatistics()
        {
            var stats = GetCurrentStatistics();

            if (!ValidateStatistics(stats))
            {
                _logger?.LogWarning("Invalid statistics detected, returning default values");
                return new CoreSerializationStatistics
                {
                    TotalOperations = 0,
                    SuccessfulOperations = 0,
                    FailedOperations = 0,
                    AverageLatency = TimeSpan.Zero,
                    LastUpdated = DateTime.UtcNow
                };
            }

            return stats;
        }
        public CacheHealthCheck PerformHealthCheck()
        {
            var healthCheck = new CacheHealthCheck
            {
                CheckTime = DateTime.UtcNow,
                IsHealthy = true,
                Issues = new List<string>()
            };

            try
            {
                // マネージャーの健全性チェック
                if (_serializerManagers.Count == 0)
                {
                    healthCheck.Issues.Add("No serializer managers cached");
                }

                // 統計の妥当性チェック
                var stats = GetValidatedStatistics();
                if (stats.TotalOperations > 0 && stats.SuccessRate < 0.5)
                {
                    healthCheck.Issues.Add($"Low success rate detected: {stats.SuccessRate:P2}");
                }

                // メモリ使用量チェック
                var memoryUsage = EstimateMemoryUsage();
                if (memoryUsage > 100 * 1024 * 1024) // 100MB
                {
                    healthCheck.Issues.Add($"High memory usage: {memoryUsage / (1024 * 1024)}MB");
                }

                // キャッシュ不整合チェック
                var usageDetails = GetCacheUsageDetails();
                if (usageDetails.TotalCacheItems > 1000)
                {
                    healthCheck.Issues.Add("Very large cache size may impact performance");
                }

                healthCheck.IsHealthy = healthCheck.Issues.Count == 0;
                healthCheck.OverallStatus = healthCheck.IsHealthy ? "Healthy" : "Issues Detected";
            }
            catch (Exception ex)
            {
                healthCheck.IsHealthy = false;
                healthCheck.OverallStatus = "Health Check Failed";
                healthCheck.Issues.Add($"Health check error: {ex.Message}");
                _logger?.LogError(ex, "Cache health check failed");
            }

            return healthCheck;
        }

        public CacheUsageDetails GetCacheUsageDetails()
        {
            return new CacheUsageDetails
            {
                ManagerCount = _serializerManagers.Count,
                SerializerCacheCount = _serializerCache.Count,
                DeserializerCacheCount = _deserializerCache.Count,
                TotalCacheItems = _serializerManagers.Count + _serializerCache.Count + _deserializerCache.Count,
                EstimatedMemoryUsage = EstimateMemoryUsage(),
                LastUpdated = DateTime.UtcNow
            };
        }
        public new void ClearCache()
        {
            ClearSerializerCache();
            ClearDeserializerCache();

            // マネージャーキャッシュもクリア
            foreach (var manager in _serializerManagers.Values.OfType<IDisposable>())
            {
                manager.Dispose();
            }
            _serializerManagers.Clear();

            _logger?.LogInformation("All Avro caches cleared");
        }

        public void ClearCache<T>() where T : class
        {
            var entityType = typeof(T);
            if (_serializerManagers.TryRemove(entityType, out var manager))
            {
                if (manager is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                ClearCache();
                _disposed = true;
            }
        }
        public int GetCachedItemCount()
        {
            return _serializerManagers.Count;
        }

        /// <summary>
        /// 型別のキャッシュ統計を取得
        /// </summary>
        public CacheStatistics GetCacheStatistics()
        {
            var avroStats = GetAvroStatistics();

            return new CacheStatistics
            {
                TotalRequests = avroStats.TotalOperations,
                CacheHits = avroStats.SuccessfulOperations,
                CacheMisses = avroStats.FailedOperations,
                CachedItemCount = _serializerManagers.Count,
                LastAccess = avroStats.LastUpdated,
                LastClear = null,
                Uptime = DateTime.UtcNow - DateTime.Today
            };
        }
        public CoreSerializationStatistics GetCurrentStatistics()
        {
            // 基底クラスの統計があれば取得
            CoreSerializationStatistics baseStats;
            try
            {
                baseStats = base.GetStatistics();
            }
            catch
            {
                // 基底クラスでGetStatisticsが実装されていない場合
                baseStats = new CoreSerializationStatistics();
            }

            // Avro固有の統計を取得
            var avroStats = GetAvroStatistics();

            // 統計を結合
            return CombineStatistics(baseStats, avroStats);
        }
        public AvroStatisticsReport GenerateStatisticsReport()
        {
            var currentStats = GetCurrentStatistics();
            var cacheStats = GetCacheStatistics();
            var typeStats = GetTypeSpecificStatistics();

            return new AvroStatisticsReport
            {
                GeneratedAt = DateTime.UtcNow,
                OverallStatistics = currentStats,
                CacheStatistics = cacheStats,
                TypeStatistics = typeStats,
                Summary = GenerateStatisticsSummary(currentStats, cacheStats),
                Recommendations = GenerateStatisticsRecommendations(currentStats, cacheStats)
            };
        }
        public class AvroStatisticsReport
        {
            public DateTime GeneratedAt { get; set; }
            public CoreSerializationStatistics OverallStatistics { get; set; } = new();
            public CacheStatistics CacheStatistics { get; set; } = new();
            public Dictionary<string, TypeSpecificStats> TypeStatistics { get; set; } = new();
            public string Summary { get; set; } = "";
            public List<string> Recommendations { get; set; } = new();

            public override string ToString()
            {
                return $"Avro Statistics Report - {Summary}";
            }
        }
        private List<string> GenerateStatisticsRecommendations(CoreSerializationStatistics overallStats, CacheStatistics cacheStats)
        {
            var recommendations = new List<string>();

            if (overallStats.SuccessRate < 0.8)
            {
                recommendations.Add($"Low success rate ({overallStats.SuccessRate:P2}). Review serialization error handling.");
            }

            if (cacheStats.HitRate < 0.7)
            {
                recommendations.Add($"Low cache hit rate ({cacheStats.HitRate:P2}). Consider cache warming or size optimization.");
            }

            if (overallStats.AverageLatency.TotalMilliseconds > 50)
            {
                recommendations.Add($"High average latency ({overallStats.AverageLatency.TotalMilliseconds:F2}ms). Optimize serialization performance.");
            }

            if (cacheStats.CachedItemCount < 3)
            {
                recommendations.Add("Very few cached items. Consider preloading frequently used serializers.");
            }

            if (recommendations.Count == 0)
            {
                recommendations.Add("All metrics are within acceptable ranges.");
            }

            return recommendations;
        }
        private string GenerateStatisticsSummary(CoreSerializationStatistics overallStats, CacheStatistics cacheStats)
        {
            return $"Total Operations: {overallStats.TotalOperations}, " +
                   $"Success Rate: {overallStats.SuccessRate:P2}, " +
                   $"Cache Hit Rate: {cacheStats.HitRate:P2}, " +
                   $"Cached Items: {cacheStats.CachedItemCount}, " +
                   $"Avg Latency: {overallStats.AverageLatency.TotalMilliseconds:F2}ms";
        }
        private CoreSerializationStatistics CombineStatistics(CoreSerializationStatistics stats1, CoreSerializationStatistics stats2)
        {
            var combined = new CoreSerializationStatistics
            {
                TotalOperations = stats1.TotalOperations + stats2.TotalOperations,
                SuccessfulOperations = stats1.SuccessfulOperations + stats2.SuccessfulOperations,
                FailedOperations = stats1.FailedOperations + stats2.FailedOperations,
                LastUpdated = DateTime.UtcNow
            };

            // 重み付き平均レイテンシ
            if (combined.TotalOperations > 0)
            {
                var totalLatencyTicks = (stats1.AverageLatency.Ticks * stats1.TotalOperations) +
                                       (stats2.AverageLatency.Ticks * stats2.TotalOperations);
                combined.AverageLatency = new TimeSpan(totalLatencyTicks / combined.TotalOperations);
            }
            else
            {
                combined.AverageLatency = TimeSpan.Zero;
            }

            return combined;
        }
        /// <summary>
        /// 全体のキャッシュ統計を取得
        /// </summary>
        public CacheStatistics GetGlobalCacheStatistics()
        {
            var totalStats = new CacheStatistics
            {
                CachedItemCount = _serializerManagers.Count,
                LastAccess = DateTime.UtcNow,
                LastClear = null,
                Uptime = DateTime.UtcNow - DateTime.Today // 簡易的な計算
            };

            long totalRequests = 0;
            long totalHits = 0;
            long totalMisses = 0;

            foreach (var kvp in _serializerManagers)
            {
                if (kvp.Value is IAvroSerializationManager<object> manager)
                {
                    var stats = manager.GetStatistics();
                    totalRequests += stats.TotalSerializations + stats.TotalDeserializations;
                    totalHits += stats.CacheHits;
                    totalMisses += stats.CacheMisses;
                }
            }

            totalStats.TotalRequests = totalRequests;
            totalStats.CacheHits = totalHits;
            totalStats.CacheMisses = totalMisses;

            return totalStats;
        }

        /// <summary>
        /// キャッシュサイズの統計情報を取得
        /// </summary>
        public CacheSizeInfo GetCacheSizeInfo()
        {
            return new CacheSizeInfo
            {
                TotalManagers = _serializerManagers.Count,
                SerializerCacheSize = _serializerCache.Count,
                DeserializerCacheSize = _deserializerCache.Count,
                EstimatedMemoryUsage = EstimateMemoryUsage()
            };
        }


        /// <summary>
        /// キャッシュをリセット（統計情報も含む）
        /// </summary>
        public void ResetCacheWithStatistics()
        {
            ClearCache();

            // 統計情報をリセット
            foreach (var manager in _serializerManagers.Values.OfType<IDisposable>())
            {
                manager.Dispose();
            }

            _statistics.TotalSerializations = 0;
            _statistics.TotalDeserializations = 0;
            _statistics.CacheHits = 0;
            _statistics.CacheMisses = 0;
            _statistics.LastUpdated = DateTime.UtcNow;

            if (_logger != null)
            {
                _logger.LogInformation("Avro serializer cache has been reset with statistics");
            }
        }

        /// <summary>
        /// キャッシュ効率の分析結果
        /// </summary>
        public CacheEfficiencyAnalysis AnalyzeCacheEfficiency()
        {
            var globalStats = GetGlobalCacheStatistics();
            var typeStats = GetCacheStatisticsByType();
            var sizeInfo = GetCacheSizeInfo();

            return new CacheEfficiencyAnalysis
            {
                AnalysisTime = DateTime.UtcNow,
                GlobalStatistics = globalStats,
                TypeStatistics = typeStats,
                SizeInfo = sizeInfo,
                EfficiencyScore = CalculateEfficiencyScore(globalStats),
                Recommendations = GenerateEfficiencyRecommendations(globalStats, typeStats, sizeInfo)
            };
        }

        /// <summary>
        /// 効率スコアを計算（0-100）
        /// </summary>
        private int CalculateEfficiencyScore(CacheStatistics stats)
        {
            if (stats.TotalRequests == 0) return 0;

            var hitRateScore = (int)(stats.HitRate * 70); // ヒット率に70%の重み
            var utilizationScore = Math.Min(30, stats.CachedItemCount * 3); // 使用率に30%の重み

            return Math.Min(100, hitRateScore + utilizationScore);
        }

        /// <summary>
        /// 効率改善の推奨事項を生成
        /// </summary>
        private List<string> GenerateEfficiencyRecommendations(
            CacheStatistics globalStats,
            Dictionary<Type, CacheStatistics> typeStats,
            CacheSizeInfo sizeInfo)
        {
            var recommendations = new List<string>();

            // ヒット率ベースの推奨事項
            if (globalStats.HitRate < 0.5)
            {
                recommendations.Add("Very low hit rate detected. Consider cache warming or increasing cache size.");
            }
            else if (globalStats.HitRate < 0.7)
            {
                recommendations.Add("Moderate hit rate. Consider optimizing frequently used serializers.");
            }

            // メモリ使用量ベースの推奨事項
            if (sizeInfo.EstimatedMemoryUsage > 50 * 1024 * 1024) // 50MB
            {
                recommendations.Add("High memory usage detected. Consider implementing cache eviction policies.");
            }

            // 型別の分析
            var inefficientTypes = typeStats.Where(kvp => kvp.Value.HitRate < 0.4 && kvp.Value.TotalRequests > 50)
                                          .Select(kvp => kvp.Key.Name)
                                          .ToList();

            if (inefficientTypes.Any())
            {
                recommendations.Add($"Low efficiency for types: {string.Join(", ", inefficientTypes)}");
            }

            if (recommendations.Count == 0)
            {
                recommendations.Add("Cache is performing optimally.");
            }

            return recommendations;
        }

        // 新しいデータ構造の定義

        /// <summary>
        /// キャッシュ統計情報
        /// </summary>
        public class CacheStatistics
        {
            public long TotalRequests { get; set; }
            public long CacheHits { get; set; }
            public long CacheMisses { get; set; }
            public int CachedItemCount { get; set; }
            public DateTime LastAccess { get; set; }
            public DateTime? LastClear { get; set; }
            public TimeSpan Uptime { get; set; }

            /// <summary>
            /// キャッシュヒット率（0.0 ～ 1.0）
            /// </summary>
            public double HitRate => TotalRequests > 0 ? (double)CacheHits / TotalRequests : 0.0;

            /// <summary>
            /// キャッシュミス率（0.0 ～ 1.0）
            /// </summary>
            public double MissRate => TotalRequests > 0 ? (double)CacheMisses / TotalRequests : 0.0;
        }

        /// <summary>
        /// キャッシュサイズ情報
        /// </summary>
        public class CacheSizeInfo
        {
            public int TotalManagers { get; set; }
            public int SerializerCacheSize { get; set; }
            public int DeserializerCacheSize { get; set; }
            public long EstimatedMemoryUsage { get; set; }
        }
        public class TypeSpecificStats
        {
            public string TypeName { get; set; } = "";
            public long TotalSerializations { get; set; }
            public long TotalDeserializations { get; set; }
            public long CacheHits { get; set; }
            public long CacheMisses { get; set; }
            public double SuccessRate { get; set; } // ✅ HitRate → SuccessRate
            public TimeSpan AverageLatency { get; set; }
            public DateTime LastAccess { get; set; }

            /// <summary>
            /// 総オペレーション数
            /// </summary>
            public long TotalOperations => TotalSerializations + TotalDeserializations;

            /// <summary>
            /// 統計の文字列表現
            /// </summary>
            public override string ToString()
            {
                return $"{TypeName} - Operations: {TotalOperations}, Success Rate: {SuccessRate:P2}, " +
                       $"Latency: {AverageLatency.TotalMilliseconds:F2}ms";
            }
        }
        public class CachePerformanceSummary
        {
            public DateTime Timestamp { get; set; }
            public int ItemCount { get; set; }
            public double SuccessRate { get; set; } // ✅ HitRate → SuccessRate
            public TimeSpan AverageLatency { get; set; }
            public long MemoryUsage { get; set; }
            public string Status { get; set; } = "";
            public DateTime LastActivity { get; set; }

            /// <summary>
            /// サマリーの文字列表現
            /// </summary>
            public override string ToString()
            {
                return $"Cache Summary - Items: {ItemCount}, Success Rate: {SuccessRate:P2}, " +
                       $"Latency: {AverageLatency.TotalMilliseconds:F2}ms, Status: {Status}";
            }
        }
        public class CacheEfficiencyInfo
        {
            public long TotalOperations { get; set; }
            public long SuccessfulOperations { get; set; }
            public long FailedOperations { get; set; }
            public TimeSpan AverageLatency { get; set; }
            public DateTime LastUpdated { get; set; }
            public int CachedItemCount { get; set; }
            public double SuccessRate { get; set; } // ✅ HitRate → SuccessRate
            public long MemoryEstimate { get; set; }
        }

        /// <summary>
        /// キャッシュ効率分析結果
        /// </summary>
        public class CacheEfficiencyAnalysis
        {
            public DateTime AnalysisTime { get; set; }
            public CacheStatistics GlobalStatistics { get; set; } = new();
            public Dictionary<Type, CacheStatistics> TypeStatistics { get; set; } = new();
            public CacheSizeInfo SizeInfo { get; set; } = new();
            public int EfficiencyScore { get; set; }
            public List<string> Recommendations { get; set; } = new();
        }

        internal class AvroEntitySerializationManager<T> : IAvroSerializationManager<T> where T : class
        {
            private readonly AvroSerializerFactory _factory;
            private readonly ILogger? _logger;
            private readonly ConcurrentDictionary<string, SerializerPair<T>> _serializerCache = new();
            private readonly ConcurrentDictionary<string, DeserializerPair<T>> _deserializerCache = new();
            private readonly SerializationStatistics _statistics = new();
            private bool _disposed = false;

            public Type EntityType => typeof(T);
            public SerializationFormat Format => SerializationFormat.Avro;

            public AvroEntitySerializationManager(
                AvroSerializerFactory factory,
                ILogger? logger = null)
            {
                _factory = factory ?? throw new ArgumentNullException(nameof(factory));
                _logger = logger;
            }

            public async Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default)
            {
                var cacheKey = GenerateCacheKey();

                if (_serializerCache.TryGetValue(cacheKey, out var cached))
                {
                    Interlocked.Increment(ref _statistics.CacheHits);
                    return cached;
                }

                Interlocked.Increment(ref _statistics.CacheMisses);

                var entityModel = GetEntityModel<T>();
                var serializers = await _factory.CreateSerializersAsync<T>(entityModel, cancellationToken);

                _serializerCache[cacheKey] = serializers;
                Interlocked.Increment(ref _statistics.TotalSerializations);

                return serializers;
            }

            public async Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default)
            {
                var cacheKey = GenerateCacheKey();

                if (_deserializerCache.TryGetValue(cacheKey, out var cached))
                {
                    Interlocked.Increment(ref _statistics.CacheHits);
                    return cached;
                }

                Interlocked.Increment(ref _statistics.CacheMisses);

                var entityModel = GetEntityModel<T>();
                var deserializers = await _factory.CreateDeserializersAsync<T>(entityModel, cancellationToken);

                _deserializerCache[cacheKey] = deserializers;
                Interlocked.Increment(ref _statistics.TotalDeserializations);

                return deserializers;
            }

            public async Task<bool> ValidateRoundTripAsync(T entity, CancellationToken cancellationToken = default)
            {
                try
                {
                    var serializers = await GetSerializersAsync(cancellationToken);
                    var deserializers = await GetDeserializersAsync(cancellationToken);

                    var context = new SerializationContext(MessageComponentType.Value, typeof(T).Name);

                    var serializedValue = serializers.ValueSerializer.Serialize(entity, context);
                    var deserializedValue = deserializers.ValueDeserializer.Deserialize(serializedValue, false, context);

                    return deserializedValue != null && deserializedValue.GetType() == typeof(T);
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Round-trip validation failed for {EntityType}", typeof(T).Name);
                    return false;
                }
            }

            public new CoreSerializationStatistics GetStatistics()
            {
                var aggregated = new CoreSerializationStatistics();

                // 各マネージャーの統計を集約
                foreach (var manager in _serializerManagers.Values)
                {
                    if (manager is IAvroSerializationManager<object> typedManager)
                    {
                        var stats = typedManager.GetStatistics();

                        // 統計情報を集約（SerializationStatistics → CoreSerializationStatistics変換）
                        aggregated.TotalOperations += stats.TotalSerializations + stats.TotalDeserializations;
                        aggregated.SuccessfulOperations += stats.TotalSerializations; // シリアライゼーション成功とみなす
                        aggregated.FailedOperations += stats.CacheMisses; // キャッシュミスを失敗とみなす

                        // 平均レイテンシの計算（重み付き平均）
                        if (aggregated.TotalOperations > 0)
                        {
                            var totalLatencyTicks = (aggregated.AverageLatency.Ticks * (aggregated.TotalOperations - (stats.TotalSerializations + stats.TotalDeserializations)))
                                                  + (stats.AverageLatency.Ticks * (stats.TotalSerializations + stats.TotalDeserializations));
                            aggregated.AverageLatency = new TimeSpan(totalLatencyTicks / aggregated.TotalOperations);
                        }
                        else
                        {
                            aggregated.AverageLatency = stats.AverageLatency;
                        }
                    }
                }

                aggregated.LastUpdated = DateTime.UtcNow;
                return aggregated;
            }


            private string GenerateCacheKey()
            {
                return $"{typeof(T).FullName}:avro";
            }

            private EntityModel GetEntityModel<TEntity>() where TEntity : class
            {
                var entityType = typeof(TEntity);
                var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
                var allProperties = entityType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

                return new EntityModel
                {
                    EntityType = entityType,
                    TopicAttribute = topicAttribute,
                    KeyProperties = keyProperties,
                    AllProperties = allProperties
                };
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _serializerCache.Clear();
                    _deserializerCache.Clear();
                    _disposed = true;
                }
            }
        }
    }
}
public class CacheUsageDetails
{
    public int ManagerCount { get; set; }
    public int SerializerCacheCount { get; set; }
    public int DeserializerCacheCount { get; set; }
    public int TotalCacheItems { get; set; }
    public long EstimatedMemoryUsage { get; set; }
    public DateTime LastUpdated { get; set; }
}

/// <summary>
/// キャッシュ健全性チェック結果
/// </summary>
public class CacheHealthCheck
{
    public DateTime CheckTime { get; set; }
    public bool IsHealthy { get; set; }
    public string OverallStatus { get; set; } = "";
    public List<string> Issues { get; set; } = new();
}

/// <summary>
/// キャッシュメンテナンス結果
/// </summary>
public class CacheMaintenanceResult
{
    public DateTime MaintenanceTime { get; set; }
    public bool Success { get; set; }
    public List<string> ActionsPerformed { get; set; } = new();
    public long MemoryFreed { get; set; }
}