using KsqlDsl.Monitoring.Diagnostics;
using KsqlDsl.Monitoring.Metrics;  // ExtendedCacheStatisticsのための修正
using System;
using System.Collections.Generic;
using System.Linq;

namespace KsqlDsl.Monitoring.Abstractions.Models
{
    /// <summary>
    /// Kafka診断情報
    /// </summary>
    public class KafkaDiagnostics
    {
        public DateTime GeneratedAt { get; set; }

        // 新しい統合設定オブジェクト
        public KafkaConfigurationSnapshot Configuration { get; set; } = new();

        public ProducerDiagnostics ProducerDiagnostics { get; set; } = new();
        public ConsumerDiagnostics ConsumerDiagnostics { get; set; } = new();
        public MonitoringCacheStatistics AvroCache { get; set; } = new();
        public Dictionary<string, object> SystemInfo { get; set; } = new();

        /// <summary>
        /// 診断情報のサマリ生成
        /// </summary>
        public string GenerateSummary()
        {
            return $@"Kafka Diagnostics Summary (Generated: {GeneratedAt:yyyy-MM-dd HH:mm:ss}):
- Producer: Active={ProducerDiagnostics.PerformanceStats.ActiveProducers}, Messages={ProducerDiagnostics.PerformanceStats.TotalMessages:N0}
- Consumer: Active={ConsumerDiagnostics.PerformanceStats.ActiveConsumers}, Messages={ConsumerDiagnostics.PerformanceStats.TotalMessages:N0}
- Avro Cache: Hit Rate={AvroCache.BaseStatistics.HitRate:P2}, Items={AvroCache.BaseStatistics.CachedItemCount:N0}
- System: Memory={GetSystemMemory():N0} bytes, Threads={GetThreadCount()}";
        }

        /// <summary>
        /// 全体的な健全性レベルの算出
        /// </summary>
        public HealthLevel CalculateOverallHealth()
        {
            var scores = new List<double>();

            // Producer健全性スコア
            var producerFailureRate = ProducerDiagnostics.PerformanceStats.FailureRate;
            var producerScore = producerFailureRate < 0.1 ? 1.0 : producerFailureRate < 0.2 ? 0.5 : 0.0;
            scores.Add(producerScore);

            // Consumer健全性スコア
            var consumerFailureRate = ConsumerDiagnostics.PerformanceStats.FailureRate;
            var consumerScore = consumerFailureRate < 0.1 ? 1.0 : consumerFailureRate < 0.2 ? 0.5 : 0.0;
            scores.Add(consumerScore);

            // Avroキャッシュ健全性スコア
            scores.Add(AvroCache.HealthScore);

            // 全体スコア計算
            var overallScore = scores.Count > 0 ? scores.Average() : 0.0;

            return overallScore switch
            {
                >= 0.8 => HealthLevel.Healthy,
                >= 0.5 => HealthLevel.Warning,
                _ => HealthLevel.Critical
            };
        }

        /// <summary>
        /// パフォーマンス推奨事項の生成
        /// </summary>
        public List<string> GetPerformanceRecommendations()
        {
            var recommendations = new List<string>();

            // Producer推奨事項
            if (ProducerDiagnostics.PerformanceStats.FailureRate > 0.1)
            {
                recommendations.Add($"Producer失敗率が高い（{ProducerDiagnostics.PerformanceStats.FailureRate:P2}）ため、接続設定やリトライ設定の見直しを推奨");
            }

            if (ProducerDiagnostics.PerformanceStats.AverageLatency.TotalMilliseconds > 100)
            {
                recommendations.Add($"Producer平均レイテンシが高い（{ProducerDiagnostics.PerformanceStats.AverageLatency.TotalMilliseconds:F0}ms）ため、バッチ設定の最適化を推奨");
            }

            // Consumer推奨事項
            if (ConsumerDiagnostics.PerformanceStats.FailureRate > 0.1)
            {
                recommendations.Add($"Consumer失敗率が高い（{ConsumerDiagnostics.PerformanceStats.FailureRate:P2}）ため、処理ロジックの見直しを推奨");
            }

            // Avroキャッシュ推奨事項
            recommendations.AddRange(AvroCache.GetOptimizationRecommendations());

            return recommendations;
        }

        private long GetSystemMemory()
        {
            return SystemInfo.TryGetValue("MemoryUsage", out var memory) && memory is long memoryLong
                ? memoryLong
                : GC.GetTotalMemory(false);
        }

        private int GetThreadCount()
        {
            return SystemInfo.TryGetValue("ThreadCount", out var threads) && threads is int threadInt
                ? threadInt
                : Environment.ProcessorCount;
        }
    }

    /// <summary>
    /// 健全性レベル
    /// </summary>
    public enum HealthLevel
    {
        Healthy,
        Warning,
        Critical
    }
}