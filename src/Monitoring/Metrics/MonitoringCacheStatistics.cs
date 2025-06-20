using KsqlDsl.Serialization.Avro.Cache;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Metrics
{
    public class MonitoringCacheStatistics
    {
        public CacheStatistics BaseStatistics { get; set; } = new();
        public long TotalOperations { get; set; }
        public long SlowOperations { get; set; }
        public double SlowOperationRate => TotalOperations > 0 ? (double)SlowOperations / TotalOperations : 0.0;
        public Dictionary<Type, EntityStatistics> EntityStatistics { get; set; } = new();
        public Dictionary<Type, PerformanceMetrics> PerformanceMetrics { get; set; } = new();
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;

        public double HealthScore
        {
            get
            {
                var hitRateScore = BaseStatistics.HitRate;
                var slowOpScore = 1.0 - SlowOperationRate;
                var uptimeScore = Math.Min(1.0, BaseStatistics.Uptime.TotalHours / 24.0);
                return (hitRateScore + slowOpScore + uptimeScore) / 3.0;
            }
        }

        public List<string> GetOptimizationRecommendations()
        {
            var recommendations = new List<string>();

            if (BaseStatistics.HitRate < 0.7)
                recommendations.Add("キャッシュヒット率が低いため、キャッシュ戦略の見直しを検討してください");

            if (SlowOperationRate > 0.1)
                recommendations.Add($"スロー操作率が高い（{SlowOperationRate:P2}）ため、パフォーマンスの最適化が必要です");

            if (BaseStatistics.CachedItemCount > 1000)
                recommendations.Add("キャッシュサイズが大きいため、メモリ使用量の監視と定期的なクリーンアップを検討してください");

            if (EntityStatistics.Count > 50)
                recommendations.Add("多数のエンティティタイプがキャッシュされています。使用頻度の低いエンティティの削除を検討してください");

            return recommendations;
        }

        public void Reset()
        {
            BaseStatistics = new CacheStatistics();
            TotalOperations = 0;
            SlowOperations = 0;
            EntityStatistics.Clear();
            PerformanceMetrics.Clear();
            LastUpdated = DateTime.UtcNow;
        }

        public string GenerateSummary()
        {
            return $@"Extended Cache Statistics Summary:
- Hit Rate: {BaseStatistics.HitRate:P2}
- Cached Items: {BaseStatistics.CachedItemCount:N0}
- Total Operations: {TotalOperations:N0}
- Slow Operation Rate: {SlowOperationRate:P2}
- Entity Types: {EntityStatistics.Count:N0}
- Health Score: {HealthScore:P2}
- Uptime: {BaseStatistics.Uptime.TotalHours:F1} hours
- Last Updated: {LastUpdated:yyyy-MM-dd HH:mm:ss}";
        }
    }

    public class EntityStatistics
    {
        public Type EntityType { get; set; } = default!;
        public long TotalRequests { get; set; }
        public long CacheHits { get; set; }
        public long CacheMisses { get; set; }
        public double HitRate => TotalRequests > 0 ? (double)CacheHits / TotalRequests : 0.0;
        public TimeSpan AverageLatency { get; set; }
        public DateTime LastAccessed { get; set; }
        public long BytesUsed { get; set; }
    }

    public class PerformanceMetrics
    {
        public Type EntityType { get; set; } = default!;
        public TimeSpan AverageDuration { get; set; }
        public TimeSpan MinDuration { get; set; }
        public TimeSpan MaxDuration { get; set; }
        public long OperationCount { get; set; }
        public double SuccessRate { get; set; }
        public DateTime LastOperation { get; set; }
        public bool IsPerformant => AverageDuration.TotalMilliseconds < 50 && SuccessRate > 0.95;
        public bool IsSlow => AverageDuration.TotalMilliseconds > 100;
    }
}