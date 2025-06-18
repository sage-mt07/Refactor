using KsqlDsl.Configuration.Options;
using KsqlDsl.Monitoring.Abstractions;
using KsqlDsl.Monitoring.Metrics;
using KsqlDsl.Serialization.Avro.Cache;        // PerformanceMonitoringAvroCache用
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Health
{
    /// <summary>
    /// Avroシリアライザーキャッシュのヘルスチェック実装
    /// 設計理由：既存AvroSerializerCacheHealthCheckからの責務移行
    /// IHealthMonitor統合により監視アーキテクチャの統一を実現
    /// </summary>
    public class AvroHealthChecker : IHealthMonitor
    {
        private readonly PerformanceMonitoringAvroCache _cache;
        private readonly AvroHealthCheckOptions _options;
        private readonly ILogger<AvroHealthChecker> _logger;
        private HealthStatus _lastStatus = HealthStatus.Unknown;

        public string ComponentName => "Avro Serializer Cache";
        public HealthLevel Level => HealthLevel.Critical;

        public event EventHandler<HealthStateChangedEventArgs>? HealthStateChanged;

        public AvroHealthChecker(
            PerformanceMonitoringAvroCache cache,
            IOptions<AvroHealthCheckOptions> options,
            ILogger<AvroHealthChecker> logger)
        {
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default)
        {
            var stopwatch = Stopwatch.StartNew();
            var result = new HealthCheckResult
            {
                CheckedAt = DateTime.UtcNow
            };

            try
            {
                var stats = _cache.GetExtendedStatistics();
                var analysisResult = await AnalyzeHealthAsync(stats, cancellationToken);

                result.Status = analysisResult.Status;
                result.Description = analysisResult.Description;
                result.Data = analysisResult.Data;
                result.Duration = stopwatch.Elapsed;

                // ヘルス状態変更の通知
                if (_lastStatus != result.Status)
                {
                    OnHealthStateChanged(_lastStatus, result.Status, analysisResult.Reason);
                    _lastStatus = result.Status;
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Status = HealthStatus.Unhealthy;
                result.Description = "Avro cache health check failed due to internal error";
                result.Exception = ex;
                result.Duration = stopwatch.Elapsed;

                _logger.LogError(ex, "Avro cache health check encountered an unexpected error");
                return result;
            }
        }

        private async Task<HealthAnalysisResult> AnalyzeHealthAsync(
            ExtendedCacheStatistics stats,
            CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken); // 非同期メソッドの形式保持

            var analysis = new HealthAnalysisResult();
            var issues = new List<string>();
            var warnings = new List<string>();

            // 基本統計の分析
            var baseStats = stats.BaseStatistics;

            // 1. キャッシュヒット率の評価
            if (baseStats.TotalRequests >= _options.MinimumRequestsForEvaluation)
            {
                if (baseStats.HitRate < _options.CriticalHitRateThreshold)
                {
                    issues.Add($"Critical: Cache hit rate too low: {baseStats.HitRate:P2}");
                }
                else if (baseStats.HitRate < _options.WarningHitRateThreshold)
                {
                    warnings.Add($"Warning: Cache hit rate below optimal: {baseStats.HitRate:P2}");
                }
            }

            // 2. スロー操作率の評価
            if (stats.TotalOperations >= _options.MinimumOperationsForEvaluation)
            {
                if (stats.SlowOperationRate > _options.CriticalSlowOperationRateThreshold)
                {
                    issues.Add($"Critical: Slow operation rate too high: {stats.SlowOperationRate:P2}");
                }
                else if (stats.SlowOperationRate > _options.WarningSlowOperationRateThreshold)
                {
                    warnings.Add($"Warning: Slow operation rate elevated: {stats.SlowOperationRate:P2}");
                }
            }

            // 3. メモリ使用量の評価
            if (baseStats.CachedItemCount > _options.CriticalCacheSizeThreshold)
            {
                issues.Add($"Critical: Cache size too large: {baseStats.CachedItemCount}");
            }
            else if (baseStats.CachedItemCount > _options.WarningCacheSizeThreshold)
            {
                warnings.Add($"Warning: Cache size growing: {baseStats.CachedItemCount}");
            }

            // 4. エンティティ別パフォーマンスの評価
            var slowEntities = 0;
            var failingEntities = 0;

            foreach (var kvp in stats.PerformanceMetrics)
            {
                var metrics = kvp.Value;
                if (metrics.AverageDuration.TotalMilliseconds > _options.CriticalAverageOperationTimeMs)
                {
                    slowEntities++;
                }
                if (metrics.SuccessRate < _options.MinimumSuccessRate)
                {
                    failingEntities++;
                }
            }

            if (slowEntities > _options.MaxSlowEntitiesBeforeCritical)
            {
                issues.Add($"Critical: Too many slow entities: {slowEntities}");
            }
            else if (slowEntities > 0)
            {
                warnings.Add($"Slow entities detected: {slowEntities}");
            }

            if (failingEntities > 0)
            {
                issues.Add($"Entities with low success rate: {failingEntities}");
            }

            // ヘルス状態の決定
            if (issues.Count > 0)
            {
                analysis.Status = HealthStatus.Unhealthy;
                analysis.Description = $"Avro Cache Critical Issues: {string.Join("; ", issues)}";
                analysis.Reason = "Critical issues detected";
            }
            else if (warnings.Count >= 3)
            {
                analysis.Status = HealthStatus.Degraded;
                analysis.Description = $"Avro Cache Warnings: {string.Join("; ", warnings)}";
                analysis.Reason = "Multiple warnings detected";
            }
            else if (warnings.Count > 0)
            {
                analysis.Status = HealthStatus.Degraded;
                analysis.Description = $"Avro Cache Warnings: {string.Join("; ", warnings)}";
                analysis.Reason = "Performance warnings";
            }
            else
            {
                analysis.Status = HealthStatus.Healthy;
                analysis.Description = "Avro Cache: All systems operational";
                analysis.Reason = "All metrics within normal range";
            }

            // 詳細データの構築
            analysis.Data = new
            {
                CacheHitRate = baseStats.HitRate,
                CachedItems = baseStats.CachedItemCount,
                TotalRequests = baseStats.TotalRequests,
                SlowOperationRate = stats.SlowOperationRate,
                SlowEntitiesCount = slowEntities,
                FailingEntitiesCount = failingEntities,
                UptimeMinutes = baseStats.Uptime.TotalMinutes,
                Issues = issues,
                Warnings = warnings
            };

            return analysis;
        }

        private void OnHealthStateChanged(HealthStatus previousStatus, HealthStatus currentStatus, string reason)
        {
            try
            {
                HealthStateChanged?.Invoke(this, new HealthStateChangedEventArgs
                {
                    PreviousStatus = previousStatus,
                    CurrentStatus = currentStatus,
                    ComponentName = ComponentName,
                    ChangedAt = DateTime.UtcNow,
                    Reason = reason
                });

                _logger.LogInformation(
                    "Avro cache health status changed: {PreviousStatus} -> {CurrentStatus}, Reason: {Reason}",
                    previousStatus, currentStatus, reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error notifying health state change");
            }
        }

        /// <summary>
        /// ヘルス分析結果
        /// </summary>
        private class HealthAnalysisResult
        {
            public HealthStatus Status { get; set; }
            public string Description { get; set; } = string.Empty;
            public string Reason { get; set; } = string.Empty;
            public object? Data { get; set; }
        }
    }
}