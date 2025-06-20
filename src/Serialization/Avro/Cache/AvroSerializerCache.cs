using Confluent.Kafka;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Models;
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
        // ✅ 追加: _avroCacheフィールドの定義
        private readonly ExtendedCacheStatistics? _avroCache;

        public Type EntityType => typeof(object);

        public AvroSerializerCache(
            AvroSerializerFactory factory,
            ILoggerFactory? logger = null)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _logger = logger?.CreateLogger<AvroSerializerCache>()
                ?? NullLogger<AvroSerializerCache>.Instance;
        }

        // ✅ 修正: _avroCacheを初期化するコンストラクタオーバーロードを追加
        public AvroSerializerCache(
            AvroSerializerFactory factory,
            ExtendedCacheStatistics avroCache,
            ILoggerFactory? logger = null) : this(factory, logger)
        {
            _avroCache = avroCache;
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
                // ✅ 修正: _avroCacheのメソッドを呼び出し
                return GenerateStatisticsReport();
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
                    var cachedItemCount = GetCachedItemCount();
                    var globalStats = GetCurrentStatistics();

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
                var cachedItemCount = GetCachedItemCount();
                var globalStats = GetCurrentStatistics();

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

        // ✅ 修正: GenerateRecommendationsメソッドを追加
        private List<string> GenerateRecommendations(double successRate, int cachedItemCount, long totalOperations)
        {
            var recommendations = new List<string>();

            if (successRate < 0.5)
            {
                recommendations.Add("Very low success rate detected. Consider cache warming or increasing cache size.");
            }
            else if (successRate < 0.7)
            {
                recommendations.Add("Moderate success rate. Consider optimizing frequently used serializers.");
            }

            if (cachedItemCount < 3)
            {
                recommendations.Add("Very few cached items. Consider preloading frequently used serializers.");
            }

            if (totalOperations > 100000)
            {
                recommendations.Add("High operation count detected. Consider cache optimization strategies.");
            }

            if (recommendations.Count == 0)
            {
                recommendations.Add("All metrics are within acceptable ranges.");
            }

            return recommendations;
        }

        // ... 残りのメソッドは既存のコードと同じ

        public int GetCachedItemCount()
        {
            return _serializerManagers.Count;
        }

        public CoreSerializationStatistics GetStatistics()
        {
            return GetAvroStatistics();
        }

        public CoreSerializationStatistics GetCurrentStatistics()
        {
            return GetAvroStatistics();
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

        // ✅ 追加: 不足しているメソッドやクラスの定義
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
                        SuccessRate = stats.HitRate,
                        AverageLatency = stats.AverageLatency,
                        LastAccess = stats.LastUpdated
                    };
                }
            }

            return typeStats;
        }

        private string GenerateStatisticsSummary(CoreSerializationStatistics overallStats, CacheStatistics cacheStats)
        {
            return $"Total Operations: {overallStats.TotalOperations}, " +
                   $"Success Rate: {overallStats.SuccessRate:P2}, " +
                   $"Cache Hit Rate: {cacheStats.HitRate:P2}, " +
                   $"Cached Items: {cacheStats.CachedItemCount}, " +
                   $"Avg Latency: {overallStats.AverageLatency.TotalMilliseconds:F2}ms";
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

        public void ClearCache()
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

        // ✅ 追加: 不足しているデータクラスの定義
        public class CacheStatistics
        {
            public long TotalRequests { get; set; }
            public long CacheHits { get; set; }
            public long CacheMisses { get; set; }
            public int CachedItemCount { get; set; }
            public DateTime LastAccess { get; set; }
            public DateTime? LastClear { get; set; }
            public TimeSpan Uptime { get; set; }

            public double HitRate => TotalRequests > 0 ? (double)CacheHits / TotalRequests : 0.0;
            public double MissRate => TotalRequests > 0 ? (double)CacheMisses / TotalRequests : 0.0;
        }

        public class TypeSpecificStats
        {
            public string TypeName { get; set; } = "";
            public long TotalSerializations { get; set; }
            public long TotalDeserializations { get; set; }
            public long CacheHits { get; set; }
            public long CacheMisses { get; set; }
            public double SuccessRate { get; set; }
            public TimeSpan AverageLatency { get; set; }
            public DateTime LastAccess { get; set; }

            public long TotalOperations => TotalSerializations + TotalDeserializations;

            public override string ToString()
            {
                return $"{TypeName} - Operations: {TotalOperations}, Success Rate: {SuccessRate:P2}, " +
                       $"Latency: {AverageLatency.TotalMilliseconds:F2}ms";
            }
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

        // ✅ 内部クラス: AvroEntitySerializationManager
        internal class AvroEntitySerializationManager<T> : IAvroSerializationManager<T> where T : class
        {
            private readonly AvroSerializerFactory _factory;
            private readonly ILogger? _logger;
            private readonly ConcurrentDictionary<string, SerializerPair<T>> _serializerCache = new();
            private readonly ConcurrentDictionary<string, DeserializerPair<T>> _deserializerCache = new();
            private readonly SerializationStatistics _statistics = new();
            private bool _disposed = false;

            public Type EntityType => typeof(T);

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

            public SerializationStatistics GetStatistics()
            {
                return _statistics;
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

    // ✅ 追加: 不足しているサポートクラス
    public class MetricsSummary
    {
        public DateTime GeneratedAt { get; set; }
        public bool CacheAvailable { get; set; }
        public int CachedItemCount { get; set; }
        public long TotalOperations { get; set; }
        public long SuccessfulOperations { get; set; }
        public long FailedOperations { get; set; }
        public double SuccessRate { get; set; }
        public TimeSpan AverageLatency { get; set; }
        public DateTime LastUpdated { get; set; }
        public string? ErrorMessage { get; set; }
    }

    public class CacheEfficiencyReport
    {
        public DateTime GeneratedAt { get; set; }
        public double OverallEfficiency { get; set; }
        public int TotalCachedItems { get; set; }
        public double OverallHitRate { get; set; }
        public double OverallMissRate { get; set; }
        public long TotalRequests { get; set; }
        public long SuccessfulOperations { get; set; }
        public long FailedOperations { get; set; }
        public TimeSpan AverageLatency { get; set; }
        public DateTime LastUpdated { get; set; }
        public List<string> Recommendations { get; set; } = new();
    }
}