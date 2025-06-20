using KsqlDsl.Configuration.Options;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Avro.Logging;
using KsqlDsl.Serialization.Avro.Metrics;
using KsqlDsl.Serialization.Avro.Performance;
using KsqlDsl.Serialization.Avro.Tracing;
using KsqlDsl.Serialization.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace KsqlDsl.Serialization.Avro.Cache
{
    public class PerformanceMonitoringAvroCache : AvroSerializerCache
    {
        private readonly ILogger<PerformanceMonitoringAvroCache>? _logger;
        private readonly PerformanceThresholds _thresholds;

        private readonly ConcurrentDictionary<string, PerformanceMetrics> _entityMetrics = new();
        private readonly ConcurrentQueue<SlowOperationRecord> _slowOperations = new();
        private readonly Timer _metricsReportTimer;
        private long _totalOperations;
        private long _slowOperationsCount;
        private DateTime _lastMetricsReport = DateTime.UtcNow;
        private readonly AvroMetricsCollector _metricsCollector;
        private bool _disposed = false;
        private readonly DateTime _startTime = DateTime.UtcNow;
        private readonly Dictionary<string, AvroSchemaInfo> _schemaCache = new();

        public PerformanceMonitoringAvroCache(
            AvroSerializerFactory factory,
            AvroMetricsCollector metricsCollector,
            ILoggerFactory? logger = null,
            PerformanceThresholds? thresholds = null)
            : base(factory, logger)
        {
            _logger = logger?.CreateLogger<PerformanceMonitoringAvroCache>()
                ?? NullLogger<PerformanceMonitoringAvroCache>.Instance;
            _thresholds = thresholds ?? new PerformanceThresholds();
            _metricsCollector = metricsCollector;
            _metricsReportTimer = new Timer(ReportMetrics, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
        }

        public override IAvroSerializer<T> GetOrCreateSerializer<T>() where T : class
        {
            var stopwatch = Stopwatch.StartNew();
            var entityTypeName = typeof(T).Name;
            using var activity = AvroActivitySource.StartCacheOperation("get_or_create_serializer", entityTypeName);

            try
            {
                var avroSerializer = base.GetOrCreateSerializer<T>();

                stopwatch.Stop();

                RecordOperation(entityTypeName, "Serializer", "Value", stopwatch.Elapsed, true);

                if (stopwatch.ElapsedMilliseconds > _thresholds.SlowSerializerCreationMs)
                {
                    RecordSlowOperation(entityTypeName, "GetOrCreateSerializer", "Value", stopwatch.Elapsed);
                    if (_logger != null)
                    {
                        AvroLogMessages.SlowSerializerCreation(
                            _logger, entityTypeName, "Serializer-Value", 0,
                            stopwatch.ElapsedMilliseconds, _thresholds.SlowSerializerCreationMs);
                    }
                }

                _metricsCollector.RecordSerializationDuration(entityTypeName, "Serializer-Value", stopwatch.Elapsed);

                return avroSerializer;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                RecordOperation(entityTypeName, "Serializer", "Value", stopwatch.Elapsed, false);
                _logger?.LogError(ex,
                    "Serializer creation failed: {EntityType} (Duration: {Duration}ms)",
                    entityTypeName, stopwatch.ElapsedMilliseconds);
                throw;
            }
        }

        public override IAvroDeserializer<T> GetOrCreateDeserializer<T>() where T : class
        {
            var stopwatch = Stopwatch.StartNew();
            var entityTypeName = typeof(T).Name;
            using var activity = AvroActivitySource.StartCacheOperation("get_or_create_deserializer", entityTypeName);

            try
            {
                var avroDeserializer = base.GetOrCreateDeserializer<T>();

                stopwatch.Stop();

                RecordOperation(entityTypeName, "Deserializer", "Value", stopwatch.Elapsed, true);

                if (stopwatch.ElapsedMilliseconds > _thresholds.SlowSerializerCreationMs)
                {
                    RecordSlowOperation(entityTypeName, "GetOrCreateDeserializer", "Value", stopwatch.Elapsed);
                    if (_logger != null)
                    {
                        AvroLogMessages.SlowSerializerCreation(
                            _logger, entityTypeName, "Deserializer-Value", 0,
                            stopwatch.ElapsedMilliseconds, _thresholds.SlowSerializerCreationMs);
                    }
                }

                _metricsCollector.RecordSerializationDuration(entityTypeName, "Deserializer-Value", stopwatch.Elapsed);

                return avroDeserializer;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                RecordOperation(entityTypeName, "Deserializer", "Value", stopwatch.Elapsed, false);
                _logger?.LogError(ex,
                    "Deserializer creation failed: {EntityType} (Duration: {Duration}ms)",
                    entityTypeName, stopwatch.ElapsedMilliseconds);
                throw;
            }
        }

        private void RecordOperation(string entityTypeName, string operationType, string serializerType, TimeSpan duration, bool success)
        {
            Interlocked.Increment(ref _totalOperations);

            var key = $"{entityTypeName}:{operationType}:{serializerType}";
            var metrics = _entityMetrics.GetOrAdd(key, _ => new PerformanceMetrics());

            lock (metrics)
            {
                metrics.OperationCount++;
                metrics.TotalDuration += duration;

                if (success)
                {
                    metrics.SuccessCount++;
                }
                else
                {
                    metrics.FailureCount++;
                }

                if (duration < metrics.MinDuration || metrics.MinDuration == TimeSpan.Zero)
                    metrics.MinDuration = duration;

                if (duration > metrics.MaxDuration)
                    metrics.MaxDuration = duration;

                metrics.AverageDuration = TimeSpan.FromTicks(metrics.TotalDuration.Ticks / metrics.OperationCount);
                metrics.LastOperation = DateTime.UtcNow;
            }
        }

        private void RecordSlowOperation(string entityTypeName, string operationType, string serializerType, TimeSpan duration)
        {
            Interlocked.Increment(ref _slowOperationsCount);

            var slowOp = new SlowOperationRecord
            {
                EntityTypeName = entityTypeName,
                OperationType = operationType,
                SerializerType = serializerType,
                Duration = duration,
                Timestamp = DateTime.UtcNow
            };

            _slowOperations.Enqueue(slowOp);

            while (_slowOperations.Count > 1000)
            {
                _slowOperations.TryDequeue(out _);
            }
        }

        public Dictionary<Type, EntityCacheStatus> GetAllEntityStatuses()
        {
            var statuses = new Dictionary<Type, EntityCacheStatus>();

            foreach (var kvp in _schemaCache)
            {
                var entityType = kvp.Value.EntityType;

                if (!statuses.ContainsKey(entityType))
                {
                    statuses[entityType] = new EntityCacheStatus
                    {
                        EntityType = entityType,
                        KeySerializerHits = 0,
                        KeySerializerMisses = 0,
                        ValueSerializerHits = kvp.Value.UsageCount,
                        ValueSerializerMisses = 0,
                        KeyDeserializerHits = 0,
                        KeyDeserializerMisses = 0,
                        ValueDeserializerHits = 0,
                        ValueDeserializerMisses = 0
                    };
                }
            }

            return statuses;
        }

        private long GetKeySerializerHits(string entityTypeName)
        {
            return _entityMetrics.TryGetValue($"{entityTypeName}:KeySerializer:Hits", out var hits)
                ? Convert.ToInt64(hits) : 0;
        }

        private long GetKeySerializerMisses(string entityTypeName)
        {
            return _entityMetrics.TryGetValue($"{entityTypeName}:KeySerializer:Misses", out var misses)
                ? Convert.ToInt64(misses) : 0;
        }

        private long GetValueSerializerHits(string entityTypeName)
        {
            return _entityMetrics.TryGetValue($"{entityTypeName}:ValueSerializer:Hits", out var hits)
                ? Convert.ToInt64(hits) : 0;
        }

        private long GetValueSerializerMisses(string entityTypeName)
        {
            return _entityMetrics.TryGetValue($"{entityTypeName}:ValueSerializer:Misses", out var misses)
                ? Convert.ToInt64(misses) : 0;
        }

        private long GetKeyDeserializerHits(string entityTypeName)
        {
            return _entityMetrics.TryGetValue($"{entityTypeName}:KeyDeserializer:Hits", out var hits)
                ? Convert.ToInt64(hits) : 0;
        }

        private long GetKeyDeserializerMisses(string entityTypeName)
        {
            return _entityMetrics.TryGetValue($"{entityTypeName}:KeyDeserializer:Misses", out var misses)
                ? Convert.ToInt64(misses) : 0;
        }

        private long GetValueDeserializerHits(string entityTypeName)
        {
            return _entityMetrics.TryGetValue($"{entityTypeName}:ValueDeserializer:Hits", out var hits)
                ? Convert.ToInt64(hits) : 0;
        }

        private long GetValueDeserializerMisses(string entityTypeName)
        {
            return _entityMetrics.TryGetValue($"{entityTypeName}:ValueDeserializer:Misses", out var misses)
                ? Convert.ToInt64(misses) : 0;
        }

        public ExtendedCacheStatistics GetExtendedStatistics()
        {
            var baseStats = GetGlobalStatistics();
            var entityStats = GetAllEntityStatuses();

            return new ExtendedCacheStatistics
            {
                BaseStatistics = baseStats,
                EntityStatistics = entityStats,
                PerformanceMetrics = GetPerformanceMetrics(),
                SlowOperations = GetRecentSlowOperations(),
                TotalOperations = _totalOperations,
                SlowOperationsCount = _slowOperationsCount,
                SlowOperationRate = _totalOperations > 0 ? (double)_slowOperationsCount / _totalOperations : 0.0,
                LastMetricsReport = _lastMetricsReport
            };
        }

        // ✅ 修正: 戻り値の型を正しく指定
        public  CacheStatistics GetGlobalStatistics()
        {
            return new CacheStatistics
            {
                TotalRequests = CalculateTotalRequests(),
                CacheHits = CalculateTotalHits(),
                CacheMisses = CalculateTotalMisses(),
                CachedItemCount = _schemaCache.Count,
                LastAccess = DateTime.UtcNow,
                LastClear = null,
                Uptime = DateTime.UtcNow - _startTime
            };
        }

        private long CalculateTotalHits()
        {
            return _entityMetrics.Values
                .Where(v => v.ToString()?.Contains("Hits") == true)
                .Sum(v => Convert.ToInt64(v));
        }

        private long CalculateTotalMisses()
        {
            return _entityMetrics.Values
                .Where(v => v.ToString()?.Contains("Misses") == true)
                .Sum(v => Convert.ToInt64(v));
        }

        private long CalculateTotalRequests()
        {
            return CalculateTotalHits() + CalculateTotalMisses();
        }

        public Dictionary<string, PerformanceMetrics> GetPerformanceMetrics()
        {
            return new Dictionary<string, PerformanceMetrics>(_entityMetrics);
        }

        public List<SlowOperationRecord> GetRecentSlowOperations(int maxCount = 100)
        {
            return _slowOperations.TakeLast(maxCount).ToList();
        }

        public void ResetPerformanceMetrics()
        {
            _entityMetrics.Clear();
            while (_slowOperations.TryDequeue(out _)) { }
            Interlocked.Exchange(ref _totalOperations, 0);
            Interlocked.Exchange(ref _slowOperationsCount, 0);
            _lastMetricsReport = DateTime.UtcNow;

            _logger?.LogInformation("Performance metrics have been reset");
        }

        private void ReportMetrics(object? state)
        {
            try
            {
                var stats = GetExtendedStatistics();

                _logger?.LogInformation(
                    "Performance Report - Total Operations: {TotalOps}, Slow Operations: {SlowOps} ({SlowRate:P2}), " +
                    "Cache Hit Rate: {HitRate:P2}, Cached Items: {CacheSize}",
                    stats.TotalOperations, stats.SlowOperationsCount, stats.SlowOperationRate,
                    stats.BaseStatistics.HitRate, stats.BaseStatistics.CachedItemCount);

                foreach (var kvp in stats.PerformanceMetrics)
                {
                    var metrics = kvp.Value;
                    if (metrics.AverageDuration.TotalMilliseconds > _thresholds.SlowSerializerCreationMs)
                    {
                        _logger?.LogWarning(
                            "Slow entity detected: {EntityKey} - Avg: {AvgMs}ms, Max: {MaxMs}ms, Operations: {Ops}",
                            kvp.Key, metrics.AverageDuration.TotalMilliseconds,
                            metrics.MaxDuration.TotalMilliseconds, metrics.OperationCount);
                    }
                }

                _lastMetricsReport = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error during performance metrics reporting");
            }
        }

        public void RegisterSchema(AvroSchemaInfo schemaInfo)
        {
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var key = GetCacheKey(schemaInfo.EntityType, schemaInfo.Type);
                _schemaCache[key] = schemaInfo;

                stopwatch.Stop();

                _metricsCollector?.RecordSchemaRegistration(
                    schemaInfo.EntityType.Name,
                    true,
                    stopwatch.Elapsed);

                _logger?.LogDebug("Schema registered: {Subject} (ID: {SchemaId})",
                    schemaInfo.Subject, schemaInfo.SchemaId);
            }
            catch (Exception ex)
            {
                stopwatch.Stop();

                _metricsCollector?.RecordSchemaRegistration(
                    schemaInfo.EntityType.Name,
                    false,
                    stopwatch.Elapsed);

                _logger?.LogError(ex, "Failed to register schema: {Subject}", schemaInfo.Subject);
                throw;
            }
        }

        private string GetCacheKey(Type entityType, SerializerType type)
        {
            return $"{entityType.FullName}:{type}";
        }

        public new void Dispose()
        {
            if (!_disposed)
            {
                _metricsReportTimer?.Dispose();
                _entityMetrics.Clear();
                while (_slowOperations.TryDequeue(out _)) { }

                base.Dispose();

                _disposed = true;
            }
        }

        // ✅ 追加: CacheStatisticsクラスを別で定義（重複排除）
        public new class CacheStatistics
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
    }
}