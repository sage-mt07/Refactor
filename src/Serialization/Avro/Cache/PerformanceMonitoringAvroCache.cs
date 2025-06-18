using KsqlDsl.Configuration.Options;
using KsqlDsl.Monitoring.Abstractions.Models;
using KsqlDsl.Monitoring.Metrics;
using KsqlDsl.Monitoring.Tracing;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace KsqlDsl.Serialization.Avro.Cache;

public class PerformanceMonitoringAvroCache : AvroSerializerCache
{
    private readonly ILogger<PerformanceMonitoringAvroCache>? _logger;
    private readonly PerformanceThresholds _thresholds;

    // パフォーマンス監視用フィールド
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
        // 定期的なメトリクスレポート（5分間隔）
        _metricsReportTimer = new Timer(ReportMetrics, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    /// <summary>
    /// パフォーマンス監視付きシリアライザー取得
    /// 設計理由：基底クラスの機能を拡張し、操作時間を測定・記録
    /// </summary>
    public override IAvroSerializer<T> GetOrCreateSerializer<T>() where T : class
    {
        var stopwatch = Stopwatch.StartNew();
        var entityTypeName = typeof(T).Name;
        using var activity = AvroActivitySource.StartCacheOperation("get_or_create_serializer", entityTypeName);

        try
        {
            // ✅ 基底クラスを引数なしで呼び出し
            var avroSerializer = base.GetOrCreateSerializer<T>();

            stopwatch.Stop();

            // パフォーマンスメトリクス記録
            RecordOperation(entityTypeName, "Serializer", "Value", stopwatch.Elapsed, true);

            // スロー操作検出
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

            // メトリクス記録
            _metricsCollector.RecordSerializationDuration(entityTypeName, "Serializer-Value", stopwatch.Elapsed);

            return avroSerializer;  // ✅ IAvroSerializer<T>をそのまま返す
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

    /// <summary>
    /// パフォーマンス監視付きデシリアライザー取得
    /// 設計理由：シリアライザーと同様の監視機能をデシリアライザーにも適用
    /// </summary>
    public override IAvroDeserializer<T> GetOrCreateDeserializer<T>() where T : class
    {
        var stopwatch = Stopwatch.StartNew();
        var entityTypeName = typeof(T).Name;
        using var activity = AvroActivitySource.StartCacheOperation("get_or_create_deserializer", entityTypeName);

        try
        {
            // ✅ 基底クラスを引数なしで呼び出し
            var avroDeserializer = base.GetOrCreateDeserializer<T>();

            stopwatch.Stop();

            // パフォーマンスメトリクス記録
            RecordOperation(entityTypeName, "Deserializer", "Value", stopwatch.Elapsed, true);

            // スロー操作検出
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

            // メトリクス記録
            _metricsCollector.RecordSerializationDuration(entityTypeName, "Deserializer-Value", stopwatch.Elapsed);

            return avroDeserializer;  // ✅ IAvroDeserializer<T>をそのまま返す
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

    /// <summary>
    /// 操作パフォーマンスの記録
    /// 設計理由：エンティティ別の詳細なパフォーマンス統計を取得
    /// </summary>
    private void RecordOperation(string entityTypeName, string operationType, string serializerType, TimeSpan duration, bool success)
    {
        Interlocked.Increment(ref _totalOperations);

        var key = $"{entityTypeName}:{operationType}:{serializerType}";
        var metrics = _entityMetrics.GetOrAdd(key, _ => new PerformanceMetrics());

        lock (metrics) // 設計理由：統計計算の原子性を保証
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

            // 最小・最大・平均値の更新
            if (duration < metrics.MinDuration || metrics.MinDuration == TimeSpan.Zero)
                metrics.MinDuration = duration;

            if (duration > metrics.MaxDuration)
                metrics.MaxDuration = duration;

            metrics.AverageDuration = TimeSpan.FromTicks(metrics.TotalDuration.Ticks / metrics.OperationCount);
            metrics.LastOperation = DateTime.UtcNow;
        }
    }

    /// <summary>
    /// スロー操作の記録
    /// 設計理由：パフォーマンス問題の詳細な分析のため、スロー操作を別途記録
    /// </summary>
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

        // スロー操作履歴の制限（最新1000件）
        // 設計理由：メモリ使用量の制御
        while (_slowOperations.Count > 1000)
        {
            _slowOperations.TryDequeue(out _);
        }
    }
    // PerformanceMonitoringAvroCache.cs に追加
    public Dictionary<Type, EntityCacheStatus> GetAllEntityStatuses()
    {
        var statuses = new Dictionary<Type, EntityCacheStatus>();

        foreach (var kvp in _schemaCache)
        {
            var entityType = kvp.Value.EntityType;  // ✅ Typeオブジェクトを直接使用

            if (!statuses.ContainsKey(entityType))
            {
                statuses[entityType] = new EntityCacheStatus  // ✅ Typeをキーに使用
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
    // PerformanceMonitoringAvroCache.cs に以下のメソッドを追加

    private long GetKeySerializerHits(string entityTypeName)
    {
        // キーシリアライザーのヒット数を取得
        // 実際の統計データがない場合はデフォルト値
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
    /// <summary>
    /// 拡張統計情報の取得
    /// 設計理由：詳細なパフォーマンス分析のための包括的な統計情報
    /// </summary>
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
    // PerformanceMonitoringAvroCache.cs に追加
    public CacheStatistics GetGlobalStatistics()
    {
        return new CacheStatistics
        {
            // ✅ 実際のプロパティ名に修正
            TotalRequests = CalculateTotalRequests(),
            CacheHits = CalculateTotalHits(),          // TotalHits → CacheHits
            CacheMisses = CalculateTotalMisses(),      // TotalMisses → CacheMisses
            CachedItemCount = _schemaCache.Count,      // CacheSize → CachedItemCount
            LastAccess = DateTime.UtcNow,             // LastUpdated → LastAccess
            LastClear = null,                         // 新規プロパティ
            Uptime = DateTime.UtcNow - _startTime     // 新規プロパティ（_startTimeが必要）
                                                      // HitRate は計算プロパティなので設定不要
                                                      // MemoryUsage プロパティは存在しないので削除
        };
    }
    // ヘルパーメソッドも実装
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

    private double CalculateHitRate()
    {
        var totalRequests = CalculateTotalRequests();
        return totalRequests > 0 ? (double)CalculateTotalHits() / totalRequests : 0.0;
    }

    private long CalculateMemoryUsage()
    {
        // 概算のメモリ使用量（バイト）
        return _schemaCache.Sum(kvp =>
            (kvp.Key.Length * 2) + // キーのメモリ使用量
            (kvp.Value.Subject.Length * 2) + // Subjectのメモリ使用量
            1024);  // その他のオーバーヘッド
    }
    /// <summary>
    /// エンティティ別パフォーマンスメトリクスの取得
    /// </summary>
    public Dictionary<string, PerformanceMetrics> GetPerformanceMetrics()
    {
        return new Dictionary<string, PerformanceMetrics>(_entityMetrics);
    }

    /// <summary>
    /// 最近のスロー操作履歴の取得
    /// </summary>
    public List<SlowOperationRecord> GetRecentSlowOperations(int maxCount = 100)
    {
        return _slowOperations.TakeLast(maxCount).ToList();
    }

    /// <summary>
    /// パフォーマンス統計のリセット
    /// 設計理由：長期運用時の統計リセット機能
    /// </summary>
    public void ResetPerformanceMetrics()
    {
        _entityMetrics.Clear();
        while (_slowOperations.TryDequeue(out _)) { }
        Interlocked.Exchange(ref _totalOperations, 0);
        Interlocked.Exchange(ref _slowOperationsCount, 0);
        _lastMetricsReport = DateTime.UtcNow;

        _logger?.LogInformation("Performance metrics have been reset");
    }

    /// <summary>
    /// 定期的なメトリクスレポート
    /// 設計理由：運用監視のための定期的な統計レポート
    /// </summary>
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

            // 低パフォーマンスエンティティの警告
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
            // キャッシュにスキーマ情報を登録
            var key = GetCacheKey(schemaInfo.EntityType, schemaInfo.Type);
            _schemaCache[key] = schemaInfo;

            stopwatch.Stop();

            // ✅ 正しいパラメータで修正
            _metricsCollector?.RecordSchemaRegistration(
                schemaInfo.EntityType.Name,
                true,  // success
                stopwatch.Elapsed);  // duration

            _logger?.LogDebug("Schema registered: {Subject} (ID: {SchemaId})",
                schemaInfo.Subject, schemaInfo.SchemaId);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            // 失敗時の記録
            _metricsCollector?.RecordSchemaRegistration(
                schemaInfo.EntityType.Name,
                false,  // success = false
                stopwatch.Elapsed);

            _logger?.LogError(ex, "Failed to register schema: {Subject}", schemaInfo.Subject);
            throw;
        }
    }

    private string GetCacheKey(Type entityType, SerializerType type)
    {
        return $"{entityType.FullName}:{type}";
    }
    /// <summary>
    /// リソース解放
    /// 設計理由：タイマーの適切な解放
    /// </summary>
    public new void Dispose()
    {
        if (!_disposed)
        {
            _metricsReportTimer?.Dispose();
            _entityMetrics.Clear();
            while (_slowOperations.TryDequeue(out _)) { }

            // 基底クラスのDispose呼び出し
            base.Dispose();

            _disposed = true;
        }
    }
}
