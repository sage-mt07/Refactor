using System;
using System.Threading;
using System.Threading.Tasks;
using KsqlDsl.Monitoring.Health;
using KsqlDsl.Monitoring.Abstractions;
using KsqlDsl.Avro;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;
using Moq;
using KsqlDsl.Monitoring.Abstractions.Models;

namespace KsqlDsl.Monitoring.Tests
{
    /// <summary>
    /// AvroHealthCheckerのテストクラス
    /// 設計理由：監視機能の品質保証、閾値ロジックの検証
    /// </summary>
    public class AvroHealthCheckerTests
    {
        private readonly Mock<PerformanceMonitoringAvroCache> _mockCache;
        private readonly Mock<ILogger<AvroHealthChecker>> _mockLogger;
        private readonly AvroHealthCheckOptions _options;

        public AvroHealthCheckerTests()
        {
            _mockCache = new Mock<PerformanceMonitoringAvroCache>();
            _mockLogger = new Mock<ILogger<AvroHealthChecker>>();
            _options = new AvroHealthCheckOptions
            {
                WarningHitRateThreshold = 0.7,
                CriticalHitRateThreshold = 0.5,
                MinimumRequestsForEvaluation = 100
            };
        }

        [Fact]
        public async Task CheckHealthAsync_HighHitRate_ReturnsHealthy()
        {
            // Arrange
            var mockStats = CreateMockStats(hitRate: 0.95, totalRequests: 1000, slowOperationRate: 0.02);
            _mockCache.Setup(c => c.GetExtendedStatistics()).Returns(mockStats);

            var healthChecker = new AvroHealthChecker(
                _mockCache.Object,
                Options.Create(_options),
                _mockLogger.Object);

            // Act
            var result = await healthChecker.CheckHealthAsync();

            // Assert
            Assert.Equal(HealthStatus.Healthy, result.Status);
            Assert.Contains("All systems operational", result.Description);
        }

        [Fact]
        public async Task CheckHealthAsync_LowHitRate_ReturnsUnhealthy()
        {
            // Arrange
            var mockStats = CreateMockStats(hitRate: 0.3, totalRequests: 1000, slowOperationRate: 0.02);
            _mockCache.Setup(c => c.GetExtendedStatistics()).Returns(mockStats);

            var healthChecker = new AvroHealthChecker(
                _mockCache.Object,
                Options.Create(_options),
                _mockLogger.Object);

            // Act
            var result = await healthChecker.CheckHealthAsync();

            // Assert
            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Contains("Critical", result.Description);
            Assert.Contains("hit rate too low", result.Description);
        }

        [Fact]
        public async Task CheckHealthAsync_WarningHitRate_ReturnsDegraded()
        {
            // Arrange
            var mockStats = CreateMockStats(hitRate: 0.6, totalRequests: 1000, slowOperationRate: 0.02);
            _mockCache.Setup(c => c.GetExtendedStatistics()).Returns(mockStats);

            var healthChecker = new AvroHealthChecker(
                _mockCache.Object,
                Options.Create(_options),
                _mockLogger.Object);

            // Act
            var result = await healthChecker.CheckHealthAsync();

            // Assert
            Assert.Equal(HealthStatus.Degraded, result.Status);
            Assert.Contains("Warnings", result.Description);
        }

        [Fact]
        public async Task CheckHealthAsync_InsufficientRequests_ReturnsHealthy()
        {
            // Arrange
            var mockStats = CreateMockStats(hitRate: 0.2, totalRequests: 50, slowOperationRate: 0.02);
            _mockCache.Setup(c => c.GetExtendedStatistics()).Returns(mockStats);

            var healthChecker = new AvroHealthChecker(
                _mockCache.Object,
                Options.Create(_options),
                _mockLogger.Object);

            // Act
            var result = await healthChecker.CheckHealthAsync();

            // Assert
            Assert.Equal(HealthStatus.Healthy, result.Status);
            Assert.Contains("All systems operational", result.Description);
        }

        [Fact]
        public async Task CheckHealthAsync_ExceptionThrown_ReturnsUnhealthy()
        {
            // Arrange
            _mockCache.Setup(c => c.GetExtendedStatistics()).Throws(new InvalidOperationException("Test exception"));

            var healthChecker = new AvroHealthChecker(
                _mockCache.Object,
                Options.Create(_options),
                _mockLogger.Object);

            // Act
            var result = await healthChecker.CheckHealthAsync();

            // Assert
            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Contains("internal error", result.Description);
            Assert.NotNull(result.Exception);
        }

        [Fact]
        public void HealthStateChanged_StatusChange_RaisesEvent()
        {
            // Arrange
            var mockStats1 = CreateMockStats(hitRate: 0.95, totalRequests: 1000, slowOperationRate: 0.02);
            var mockStats2 = CreateMockStats(hitRate: 0.3, totalRequests: 1000, slowOperationRate: 0.02);

            _mockCache.SetupSequence(c => c.GetExtendedStatistics())
                .Returns(mockStats1)
                .Returns(mockStats2);

            var healthChecker = new AvroHealthChecker(
                _mockCache.Object,
                Options.Create(_options),
                _mockLogger.Object);

            HealthStateChangedEventArgs? eventArgs = null;
            healthChecker.HealthStateChanged += (sender, args) => eventArgs = args;

            // Act
            _ = healthChecker.CheckHealthAsync().Result; // First check - Healthy
            _ = healthChecker.CheckHealthAsync().Result; // Second check - Unhealthy

            // Assert
            Assert.NotNull(eventArgs);
            Assert.Equal(HealthStatus.Healthy, eventArgs.PreviousStatus);
            Assert.Equal(HealthStatus.Unhealthy, eventArgs.CurrentStatus);
            Assert.Equal("Avro Serializer Cache", eventArgs.ComponentName);
        }

        [Theory]
        [InlineData(0.95, 0.02, HealthStatus.Healthy)]
        [InlineData(0.65, 0.02, HealthStatus.Degraded)]
        [InlineData(0.4, 0.02, HealthStatus.Unhealthy)]
        [InlineData(0.8, 0.16, HealthStatus.Unhealthy)]
        public async Task CheckHealthAsync_VariousMetrics_ReturnsExpectedStatus(
            double hitRate,
            double slowOpRate,
            HealthStatus expectedStatus)
        {
            // Arrange
            var mockStats = CreateMockStats(hitRate, 1000, slowOpRate);
            _mockCache.Setup(c => c.GetExtendedStatistics()).Returns(mockStats);

            var healthChecker = new AvroHealthChecker(
                _mockCache.Object,
                Options.Create(_options),
                _mockLogger.Object);

            // Act
            var result = await healthChecker.CheckHealthAsync();

            // Assert
            Assert.Equal(expectedStatus, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_CancellationRequested_ThrowsOperationCanceledException()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            cts.Cancel();

            _mockCache.Setup(c => c.GetExtendedStatistics()).Returns(() =>
            {
                cts.Token.ThrowIfCancellationRequested();
                return CreateMockStats(0.8, 1000, 0.02);
            });

            var healthChecker = new AvroHealthChecker(
                _mockCache.Object,
                Options.Create(_options),
                _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                healthChecker.CheckHealthAsync(cts.Token));
        }

        private ExtendedCacheStatistics CreateMockStats(double hitRate, long totalRequests, double slowOperationRate)
        {
            var baseStats = new CacheStatistics
            {
                HitRate = hitRate,
                TotalRequests = totalRequests,
                CacheHits = (long)(totalRequests * hitRate),
                CacheMisses = (long)(totalRequests * (1 - hitRate)),
                CachedItemCount = 100,
                Uptime = TimeSpan.FromHours(1)
            };

            return new ExtendedCacheStatistics
            {
                BaseStatistics = baseStats,
                SlowOperationRate = slowOperationRate,
                TotalOperations = 500,
                SlowOperationsCount = (long)(500 * slowOperationRate),
                PerformanceMetrics = new System.Collections.Generic.Dictionary<string, PerformanceMetrics>(),
                EntityStatistics = new System.Collections.Generic.Dictionary<Type, EntityCacheStatus>(),
                SlowOperations = new System.Collections.Generic.List<SlowOperationRecord>(),
                LastMetricsReport = DateTime.UtcNow
            };
        }
    }
}