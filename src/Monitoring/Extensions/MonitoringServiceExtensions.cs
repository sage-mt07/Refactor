using KsqlDsl.Monitoring.Metrics;
using KsqlDsl.Serialization.Avro.Cache;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;

namespace KsqlDsl.Monitoring.Extensions
{
    /// <summary>
    /// モニタリングサービス全体のDI拡張メソッド
    /// 設計原則: Monitoring層として統合的な設定を提供
    /// </summary>
    public static class MonitoringServiceExtensions
    {
        /// <summary>
        /// KsqlDslモニタリング機能を一括で追加
        /// </summary>
        public static IServiceCollection AddKsqlDslMonitoring(
            this IServiceCollection services,
            Action<KsqlDslMonitoringOptions>? configure = null)
        {
            var options = new KsqlDslMonitoringOptions();
            configure?.Invoke(options);

            // 設定をサービスに登録
            services.AddSingleton(options);

            // Avroメトリクス収集
            if (options.EnableAvroMetrics)
            {
                services.AddAvroMetrics(options.EnableDebugLogging);
            }

            // ヘルスチェック（将来実装）
            if (options.EnableHealthChecks)
            {
                // services.AddHealthChecks()
                //     .AddCheck<KafkaHealthCheck>("kafka")
                //     .AddCheck<AvroHealthCheck>("avro");
            }

            // 分散トレーシング（将来実装）
            if (options.EnableTracing)
            {
                // services.AddOpenTelemetry()
                //     .WithTracing(builder => builder.AddSource("KsqlDsl"));
            }

            return services;
        }

        /// <summary>
        /// 開発環境向けの詳細モニタリング設定
        /// </summary>
        public static IServiceCollection AddKsqlDslDevelopmentMonitoring(
            this IServiceCollection services)
        {
            return services.AddKsqlDslMonitoring(options =>
            {
                options.EnableAvroMetrics = true;
                options.EnableDebugLogging = true;
                options.EnableHealthChecks = true;
                options.EnableTracing = true;
                options.MetricsCollectionInterval = TimeSpan.FromSeconds(30);
                options.ReportGenerationInterval = TimeSpan.FromMinutes(2);
            });
        }

        /// <summary>
        /// 本番環境向けの最適化されたモニタリング設定
        /// </summary>
        public static IServiceCollection AddKsqlDslProductionMonitoring(
            this IServiceCollection services)
        {
            return services.AddKsqlDslMonitoring(options =>
            {
                options.EnableAvroMetrics = true;
                options.EnableDebugLogging = false;
                options.EnableHealthChecks = true;
                options.EnableTracing = false; // 本番では無効化してパフォーマンス重視
                options.MetricsCollectionInterval = TimeSpan.FromMinutes(5);
                options.ReportGenerationInterval = TimeSpan.FromMinutes(15);
            });
        }

        /// <summary>
        /// 最小限のモニタリング設定（軽量版）
        /// </summary>
        public static IServiceCollection AddKsqlDslMinimalMonitoring(
            this IServiceCollection services)
        {
            return services.AddKsqlDslMonitoring(options =>
            {
                options.EnableAvroMetrics = true;
                options.EnableDebugLogging = false;
                options.EnableHealthChecks = false;
                options.EnableTracing = false;
                options.MetricsCollectionInterval = TimeSpan.FromMinutes(10);
                options.ReportGenerationInterval = TimeSpan.FromMinutes(30);
            });
        }
    }

    /// <summary>
    /// KsqlDslモニタリング全体のオプション
    /// </summary>
    public class KsqlDslMonitoringOptions
    {
        /// <summary>
        /// Avroメトリクス収集を有効にするか
        /// </summary>
        public bool EnableAvroMetrics { get; set; } = true;

        /// <summary>
        /// デバッグログを有効にするか
        /// </summary>
        public bool EnableDebugLogging { get; set; } = false;

        /// <summary>
        /// ヘルスチェックを有効にするか
        /// </summary>
        public bool EnableHealthChecks { get; set; } = true;

        /// <summary>
        /// 分散トレーシングを有効にするか
        /// </summary>
        public bool EnableTracing { get; set; } = false;

        /// <summary>
        /// メトリクス収集間隔
        /// </summary>
        public TimeSpan MetricsCollectionInterval { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// キャッシュ効率レポートの生成間隔
        /// </summary>
        public TimeSpan ReportGenerationInterval { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// メトリクス保持期間
        /// </summary>
        public TimeSpan MetricsRetentionPeriod { get; set; } = TimeSpan.FromHours(24);

        /// <summary>
        /// アラート閾値設定
        /// </summary>
        public AlertThresholds AlertThresholds { get; set; } = new();
    }

    /// <summary>
    /// アラート閾値設定
    /// </summary>
    public class AlertThresholds
    {
        /// <summary>
        /// キャッシュヒット率の警告閾値
        /// </summary>
        public double CacheHitRateWarningThreshold { get; set; } = 0.7;

        /// <summary>
        /// キャッシュヒット率の重大閾値
        /// </summary>
        public double CacheHitRateCriticalThreshold { get; set; } = 0.5;

        /// <summary>
        /// 平均レイテンシの警告閾値（ミリ秒）
        /// </summary>
        public double AverageLatencyWarningMs { get; set; } = 50.0;

        /// <summary>
        /// 平均レイテンシの重大閾値（ミリ秒）
        /// </summary>
        public double AverageLatencyCriticalMs { get; set; } = 100.0;

        /// <summary>
        /// 失敗率の警告閾値
        /// </summary>
        public double FailureRateWarningThreshold { get; set; } = 0.1;

        /// <summary>
        /// 失敗率の重大閾値
        /// </summary>
        public double FailureRateCriticalThreshold { get; set; } = 0.2;
    }
}