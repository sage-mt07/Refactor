using KsqlDsl.Serialization.Avro.Cache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace KsqlDsl.Monitoring.Metrics
{
    /// <summary>
    /// Avroシリアライゼーションのメトリクス収集（互換性修正版）
    /// </summary>
    public class AvroMetricsCollector : IDisposable
    {
        private readonly ILogger<AvroMetricsCollector>? _logger;
        private readonly AvroSerializerCache? _avroCache;
        private readonly bool _enableDebugLogging;
        private readonly Meter _meter;

        // メトリクス定義（修正版）
        private readonly Counter<long> _serializationCounter;
        private readonly Counter<long> _deserializationCounter;
        private readonly Histogram<double> _serializationDuration;
        private readonly Histogram<double> _deserializationDuration;
        private readonly ObservableGauge<int> _cacheItemsGauge; // ✅ ObservableGaugeを使用
        private readonly ObservableGauge<double> _cacheHitRateGauge; // ✅ ObservableGaugeを使用
        private readonly ObservableGauge<double> _cacheMissRateGauge; // ✅ ObservableGaugeを使用
        private readonly Counter<long> _schemaRegistrationCounter;
        private readonly Histogram<double> _schemaRegistrationDuration;

        private bool _disposed = false;

        public AvroMetricsCollector(
            AvroSerializerCache? avroCache = null,
            ILogger<AvroMetricsCollector>? logger = null,
            bool enableDebugLogging = false)
        {
            _avroCache = avroCache;
            _logger = logger;
            _enableDebugLogging = enableDebugLogging;

            _meter = new Meter("KsqlDsl.Avro", "1.0.0");

            // カウンターとヒストグラムの初期化
            _serializationCounter = _meter.CreateCounter<long>(
                "avro_serializations_total",
                "count",
                "Total number of Avro serializations");

            _deserializationCounter = _meter.CreateCounter<long>(
                "avro_deserializations_total",
                "count",
                "Total number of Avro deserializations");

            _serializationDuration = _meter.CreateHistogram<double>(
                "avro_serialization_duration_ms",
                "milliseconds",
                "Duration of Avro serialization operations");

            _deserializationDuration = _meter.CreateHistogram<double>(
                "avro_deserialization_duration_ms",
                "milliseconds",
                "Duration of Avro deserialization operations");

            _schemaRegistrationCounter = _meter.CreateCounter<long>(
                "avro_schema_registrations_total",
                "count",
                "Total number of schema registrations");

            _schemaRegistrationDuration = _meter.CreateHistogram<double>(
                "avro_schema_registration_duration_ms",
                "milliseconds",
                "Duration of schema registration operations");

            // ✅ ObservableGaugeの初期化（コールバック関数を提供）
            _cacheItemsGauge = _meter.CreateObservableGauge<int>(
                "avro_cache_items_count",
                () => GetCacheItemCount(),
                "count",
                "Number of items in Avro cache");

            _cacheHitRateGauge = _meter.CreateObservableGauge<double>(
                "avro_cache_hit_rate",
                () => GetCacheHitRate(),
                "ratio",
                "Avro cache hit rate");

            _cacheMissRateGauge = _meter.CreateObservableGauge<double>(
                "avro_cache_miss_rate",
                () => GetCacheMissRate(),
                "ratio",
                "Avro cache miss rate");

            _logger?.LogDebug("AvroMetricsCollector initialized with cache: {HasCache}", _avroCache != null);
        }

        /// <summary>
        /// キャッシュアイテム数を取得（ObservableGauge用）
        /// </summary>
        private int GetCacheItemCount()
        {
            try
            {
                return _avroCache?.GetCachedItemCount() ?? 0;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to get cache item count");
                return 0;
            }
        }

        /// <summary>
        /// キャッシュヒット率を取得（ObservableGauge用）
        /// </summary>
        private double GetCacheHitRate()
        {
            try
            {
                if (_avroCache == null) return 0.0;
                var stats = _avroCache.GetStatistics();
                return stats.SuccessRate;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to get cache hit rate");
                return 0.0;
            }
        }

        /// <summary>
        /// キャッシュミス率を取得（ObservableGauge用）
        /// </summary>
        private double GetCacheMissRate()
        {
            try
            {
                if (_avroCache == null) return 0.0;
                var stats = _avroCache.GetStatistics();
                return stats.FailureRate;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to get cache miss rate");
                return 0.0;
            }
        }

        /// <summary>
        /// シリアライゼーション操作を記録
        /// </summary>
        public void RecordSerialization(string entityType, bool success, TimeSpan duration)
        {
            try
            {
                var tags = new KeyValuePair<string, object?>[]
                {
                    new("entity_type", entityType),
                    new("success", success)
                };

                _serializationCounter.Add(1, tags);
                _serializationDuration.Record(duration.TotalMilliseconds, tags);

                if (_enableDebugLogging)
                {
                    _logger?.LogDebug(
                        "Recorded serialization: Entity={EntityType}, Success={Success}, Duration={Duration}ms",
                        entityType, success, duration.TotalMilliseconds);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to record serialization metrics");
            }
        }

        /// <summary>
        /// デシリアライゼーション操作を記録
        /// </summary>
        public void RecordDeserialization(string entityType, bool success, TimeSpan duration)
        {
            try
            {
                var tags = new KeyValuePair<string, object?>[]
                {
                    new("entity_type", entityType),
                    new("success", success)
                };

                _deserializationCounter.Add(1, tags);
                _deserializationDuration.Record(duration.TotalMilliseconds, tags);

                if (_enableDebugLogging)
                {
                    _logger?.LogDebug(
                        "Recorded deserialization: Entity={EntityType}, Success={Success}, Duration={Duration}ms",
                        entityType, success, duration.TotalMilliseconds);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to record deserialization metrics");
            }
        }

        /// <summary>
        /// スキーマ登録操作を記録
        /// </summary>
        public void RecordSchemaRegistration(string entityType, bool success, TimeSpan duration)
        {
            try
            {
                var tags = new KeyValuePair<string, object?>[]
                {
                    new("entity_type", entityType),
                    new("success", success)
                };

                _schemaRegistrationCounter.Add(1, tags);
                _schemaRegistrationDuration.Record(duration.TotalMilliseconds, tags);

                if (_enableDebugLogging)
                {
                    _logger?.LogDebug(
                        "Recorded schema registration: Entity={EntityType}, Success={Success}, Duration={Duration}ms",
                        entityType, success, duration.TotalMilliseconds);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to record schema registration metrics");
            }
        }

        /// <summary>
        /// 手動でキャッシュ統計を強制収集（オプション）
        /// </summary>
        public void ForceCollectCacheStatistics()
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
                // ObservableGaugeは自動的に値を収集するため、ここでは統計ログのみ
                var cachedItemCount = _avroCache.GetCachedItemCount();
                var globalStats = _avroCache.GetStatistics();

                if (_enableDebugLogging)
                {
                    _logger?.LogDebug(
                        "Cache statistics: Items={CachedItems}, SuccessRate={SuccessRate:P2}, Operations={TotalOps}",
                        cachedItemCount, globalStats.SuccessRate, globalStats.TotalOperations);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to force collect cache statistics");
            }
        }

        /// <summary>
        /// キャッシュ効率レポートを生成
        /// </summary>
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
                var globalStats = _avroCache.GetStatistics();

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

        /// <summary>
        /// パフォーマンス改善の推奨事項を生成
        /// </summary>
        private List<string> GenerateRecommendations(double successRate, int cachedItemCount, long totalOperations)
        {
            var recommendations = new List<string>();

            if (successRate < 0.5)
            {
                recommendations.Add($"Very low success rate ({successRate:P2}). Investigate serialization failures and consider cache warming strategies.");
            }
            else if (successRate < 0.7)
            {
                recommendations.Add($"Moderate success rate ({successRate:P2}). Consider optimizing frequently used serializers and error handling.");
            }
            else if (successRate >= 0.95)
            {
                recommendations.Add($"Excellent success rate ({successRate:P2}). Cache is performing optimally.");
            }

            if (cachedItemCount < 3)
            {
                recommendations.Add($"Cache contains only {cachedItemCount} items. Consider preloading frequently used serializers.");
            }
            else if (cachedItemCount > 50)
            {
                recommendations.Add($"Cache contains {cachedItemCount} items. Monitor memory usage and consider implementing cache eviction policies.");
            }

            if (totalOperations > 1000 && successRate < 0.8)
            {
                recommendations.Add($"High operation volume ({totalOperations}) with suboptimal success rate. Consider implementing more robust error handling and retry mechanisms.");
            }
            else if (totalOperations < 10)
            {
                recommendations.Add("Low operation volume detected. Cache benefits may be minimal with current usage patterns.");
            }

            if (cachedItemCount > 0 && totalOperations / cachedItemCount > 1000)
            {
                recommendations.Add("High operations per cached item ratio. Cache is being effectively utilized.");
            }

            if (recommendations.Count == 0)
            {
                recommendations.Add("Cache performance is within acceptable ranges. Continue monitoring for optimal performance.");
            }

            return recommendations;
        }

        /// <summary>
        /// 詳細なメトリクス要約を取得
        /// </summary>
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
                    var globalStats = _avroCache.GetStatistics();

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

        /// <summary>
        /// メトリクスの状態診断を実行
        /// </summary>
        public MetricsDiagnostics PerformDiagnostics()
        {
            var diagnostics = new MetricsDiagnostics
            {
                DiagnosticsTime = DateTime.UtcNow,
                MeterStatus = _meter != null ? "Active" : "Inactive",
                CacheStatus = _avroCache != null ? "Available" : "Not Available"
            };

            try
            {
                // メトリクス機能の診断
                diagnostics.CountersActive = _serializationCounter != null && _deserializationCounter != null;
                diagnostics.HistogramsActive = _serializationDuration != null && _deserializationDuration != null;
                diagnostics.GaugesActive = _cacheItemsGauge != null && _cacheHitRateGauge != null && _cacheMissRateGauge != null;

                // キャッシュの診断
                if (_avroCache != null)
                {
                    var itemCount = _avroCache.GetCachedItemCount();
                    var stats = _avroCache.GetStatistics();

                    diagnostics.CacheItemCount = itemCount;
                    diagnostics.CacheOperationCount = stats.TotalOperations;
                    diagnostics.CacheSuccessRate = stats.SuccessRate;
                    diagnostics.CacheHealthy = stats.SuccessRate > 0.5 && itemCount >= 0;
                }

                diagnostics.OverallHealthy = diagnostics.CountersActive &&
                                          diagnostics.HistogramsActive &&
                                          diagnostics.GaugesActive &&
                                          diagnostics.CacheHealthy;

                if (diagnostics.OverallHealthy)
                {
                    diagnostics.HealthMessage = "All metrics systems are functioning correctly";
                }
                else
                {
                    var issues = new List<string>();
                    if (!diagnostics.CountersActive) issues.Add("Counters inactive");
                    if (!diagnostics.HistogramsActive) issues.Add("Histograms inactive");
                    if (!diagnostics.GaugesActive) issues.Add("Gauges inactive");
                    if (!diagnostics.CacheHealthy) issues.Add("Cache unhealthy");

                    diagnostics.HealthMessage = $"Issues detected: {string.Join(", ", issues)}";
                }
            }
            catch (Exception ex)
            {
                diagnostics.OverallHealthy = false;
                diagnostics.HealthMessage = $"Diagnostics failed: {ex.Message}";
                _logger?.LogError(ex, "Failed to perform metrics diagnostics");
            }

            return diagnostics;
        }

        /// <summary>
        /// リソースの解放
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                try
                {
                    _meter?.Dispose();
                    _logger?.LogDebug("AvroMetricsCollector disposed");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Error during AvroMetricsCollector disposal");
                }
                finally
                {
                    _disposed = true;
                }
            }
        }
    }

    /// <summary>
    /// キャッシュ効率レポート
    /// </summary>
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

        public override string ToString()
        {
            return $"Cache Report - Items: {TotalCachedItems}, Hit Rate: {OverallHitRate:P2}, " +
                   $"Requests: {TotalRequests}, Avg Latency: {AverageLatency.TotalMilliseconds:F2}ms";
        }
    }

    /// <summary>
    /// メトリクス要約
    /// </summary>
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

        public override string ToString()
        {
            if (!string.IsNullOrEmpty(ErrorMessage))
                return $"Metrics Summary - Error: {ErrorMessage}";

            return $"Metrics Summary - Cache: {(CacheAvailable ? "Available" : "N/A")}, " +
                   $"Items: {CachedItemCount}, Success Rate: {SuccessRate:P2}";
        }
    }

    /// <summary>
    /// メトリクス診断情報
    /// </summary>
    public class MetricsDiagnostics
    {
        public DateTime DiagnosticsTime { get; set; }
        public string MeterStatus { get; set; } = "";
        public string CacheStatus { get; set; } = "";
        public bool CountersActive { get; set; }
        public bool HistogramsActive { get; set; }
        public bool GaugesActive { get; set; }
        public int CacheItemCount { get; set; }
        public long CacheOperationCount { get; set; }
        public double CacheSuccessRate { get; set; }
        public bool CacheHealthy { get; set; }
        public bool OverallHealthy { get; set; }
        public string HealthMessage { get; set; } = "";

        public override string ToString()
        {
            return $"Metrics Diagnostics - Overall: {(OverallHealthy ? "Healthy" : "Issues")}, " +
                   $"Cache: {CacheStatus}, Message: {HealthMessage}";
        }
    }
}
