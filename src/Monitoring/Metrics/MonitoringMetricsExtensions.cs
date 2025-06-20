using KsqlDsl.Serialization.Avro.Cache;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;

namespace KsqlDsl.Monitoring.Metrics
{
    /// <summary>
    /// メトリクス関連のDI拡張メソッド
    /// </summary>
    public static class MonitoringMetricsExtensions
    {
        /// <summary>
        /// Avroメトリクス機能を追加
        /// </summary>
        public static IServiceCollection AddAvroMetrics(
            this IServiceCollection services,
            bool enableDebugLogging = false)
        {
            services.AddSingleton<MonitoringCacheStatistics>();

            // PerformanceMonitoringAvroCacheの登録（すでに存在する場合はスキップ）
            services.AddSingleton(provider =>
            {
                var loggerFactory = provider.GetService<ILoggerFactory>();
                var logger = loggerFactory?.CreateLogger<PerformanceMonitoringAvroCache>();

                return new PerformanceMonitoringAvroCache(
                    enableDebugLogging: enableDebugLogging,
                    logger: logger);
            });

            // メトリクス収集サービス
            services.AddSingleton<IMetricsCollectionService, MetricsCollectionService>();

            return services;
        }

        /// <summary>
        /// メトリクス収集設定
        /// </summary>
        public static IServiceCollection ConfigureMetricsCollection(
            this IServiceCollection services,
            Action<MetricsCollectionOptions> configure)
        {
            services.Configure(configure);
            return services;
        }
    }

    /// <summary>
    /// メトリクス収集サービスのインターフェース
    /// </summary>
    public interface IMetricsCollectionService
    {
        /// <summary>
        /// メトリクス収集の開始
        /// </summary>
        void StartCollection();

        /// <summary>
        /// メトリクス収集の停止
        /// </summary>
        void StopCollection();

        MonitoringCacheStatistics GetCurrentStatistics();
    }

    public class MetricsCollectionService : IMetricsCollectionService
    {
        private readonly MonitoringCacheStatistics _statistics;
        private readonly ILogger<MetricsCollectionService> _logger;
        private bool _isCollecting = false;

        public MetricsCollectionService(
            MonitoringCacheStatistics statistics,
            ILogger<MetricsCollectionService> logger)
        {
            _statistics = statistics ?? throw new ArgumentNullException(nameof(statistics));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public void StartCollection()
        {
            if (!_isCollecting)
            {
                _isCollecting = true;
                _logger.LogInformation("Metrics collection started");
            }
        }

        public void StopCollection()
        {
            if (_isCollecting)
            {
                _isCollecting = false;
                _logger.LogInformation("Metrics collection stopped");
            }
        }

        public MonitoringCacheStatistics GetCurrentStatistics()
        {
            return _statistics;
        }
    }

    public class MetricsCollectionOptions
    {
        public TimeSpan CollectionInterval { get; set; } = TimeSpan.FromMinutes(1);
        public bool EnableDebugLogging { get; set; } = false;
        public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromHours(24);
        public bool EnableAutoOptimization { get; set; } = true;
    }
}