using System;
using System.Collections.Generic;
using System.Linq;
using KsqlDsl.Monitoring.Metrics;
using KsqlDsl.Monitoring.Abstractions;
using Xunit;

namespace KsqlDsl.Monitoring.Tests
{
    /// <summary>
    /// AvroMetricsCollectorのテストクラス
    /// 設計理由：メトリクス収集機能の品質保証、データ整合性検証
    /// </summary>
    public class AvroMetricsCollectorTests
    {
        [Fact]
        public void Constructor_SetsInitialProperties()
        {
            // Act
            var collector = new AvroMetricsCollector();

            // Assert
            Assert.Equal("Avro", collector.TargetTypeName);
            Assert.True(DateTime.UtcNow - collector.StartTime < TimeSpan.FromSeconds(1));
        }

        [Fact]
        public void RecordCounter_NewMetric_CreatesCounter()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var tags = new Dictionary<string, object?> { ["entity_type"] = "TestEntity" };

            // Act
            collector.RecordCounter("test_counter", 5, tags);

            // Assert
            var snapshot = collector.GetSnapshot();
            Assert.Single(snapshot.Counters);

            var counter = snapshot.Counters.First().Value;
            Assert.Equal("test_counter", counter.Name);
            Assert.Equal(5, counter.Value);
            Assert.Equal("TestEntity", counter.Tags["entity_type"]);
        }

        [Fact]
        public void RecordCounter_ExistingMetric_IncrementsValue()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var tags = new Dictionary<string, object?> { ["entity_type"] = "TestEntity" };

            // Act
            collector.RecordCounter("test_counter", 5, tags);
            collector.RecordCounter("test_counter", 3, tags);

            // Assert
            var snapshot = collector.GetSnapshot();
            Assert.Single(snapshot.Counters);

            var counter = snapshot.Counters.First().Value;
            Assert.Equal(8, counter.Value); // 5 + 3
        }

        [Fact]
        public void RecordGauge_NewMetric_CreatesGauge()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var tags = new Dictionary<string, object?> { ["component"] = "cache" };

            // Act
            collector.RecordGauge("hit_rate", 0.75, tags);

            // Assert
            var snapshot = collector.GetSnapshot();
            Assert.Single(snapshot.Gauges);

            var gauge = snapshot.Gauges.First().Value;
            Assert.Equal("hit_rate", gauge.Name);
            Assert.Equal(0.75, gauge.Value);
            Assert.Equal("cache", gauge.Tags["component"]);
        }

        [Fact]
        public void RecordGauge_ExistingMetric_OverwritesValue()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var tags = new Dictionary<string, object?> { ["component"] = "cache" };

            // Act
            collector.RecordGauge("hit_rate", 0.75, tags);
            collector.RecordGauge("hit_rate", 0.85, tags);

            // Assert
            var snapshot = collector.GetSnapshot();
            Assert.Single(snapshot.Gauges);

            var gauge = snapshot.Gauges.First().Value;
            Assert.Equal(0.85, gauge.Value); // Latest value
        }

        [Fact]
        public void RecordHistogram_MultipleValues_CalculatesStatistics()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var tags = new Dictionary<string, object?> { ["operation"] = "serialize" };

            // Act
            collector.RecordHistogram("duration", 10.0, tags);
            collector.RecordHistogram("duration", 20.0, tags);
            collector.RecordHistogram("duration", 30.0, tags);

            // Assert
            var snapshot = collector.GetSnapshot();
            Assert.Single(snapshot.Histograms);

            var histogram = snapshot.Histograms.First().Value;
            Assert.Equal("duration", histogram.Name);
            Assert.Equal(3, histogram.Count);
            Assert.Equal(60.0, histogram.Sum);
            Assert.Equal(10.0, histogram.Min);
            Assert.Equal(30.0, histogram.Max);
            Assert.Equal(20.0, histogram.Average);
        }

        [Fact]
        public void RecordTimer_MultipleValues_CalculatesStatistics()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var tags = new Dictionary<string, object?> { ["operation"] = "deserialize" };

            // Act
            collector.RecordTimer("processing_time", TimeSpan.FromMilliseconds(100), tags);
            collector.RecordTimer("processing_time", TimeSpan.FromMilliseconds(200), tags);
            collector.RecordTimer("processing_time", TimeSpan.FromMilliseconds(300), tags);

            // Assert
            var snapshot = collector.GetSnapshot();
            Assert.Single(snapshot.Timers);

            var timer = snapshot.Timers.First().Value;
            Assert.Equal("processing_time", timer.Name);
            Assert.Equal(3, timer.Count);
            Assert.Equal(TimeSpan.FromMilliseconds(600), timer.TotalTime);
            Assert.Equal(TimeSpan.FromMilliseconds(100), timer.MinTime);
            Assert.Equal(TimeSpan.FromMilliseconds(300), timer.MaxTime);
            Assert.Equal(TimeSpan.FromMilliseconds(200), timer.AverageTime);
        }

        [Fact]
        public void RecordCacheHit_CallsCorrectMethods()
        {
            // Arrange
            var collector = new AvroMetricsCollector();

            // Act
            collector.RecordCacheHit("TestEntity", "Value");

            // Assert
            var snapshot = collector.GetSnapshot();

            // Counter should be recorded
            Assert.Contains(snapshot.Counters, kvp => kvp.Value.Name == "cache_hits");
            var counter = snapshot.Counters.First(kvp => kvp.Value.Name == "cache_hits").Value;
            Assert.Equal(1, counter.Value);
            Assert.Equal("TestEntity", counter.Tags["entity_type"]);
            Assert.Equal("Value", counter.Tags["serializer_type"]);
        }

        [Fact]
        public void RecordCacheMiss_CallsCorrectMethods()
        {
            // Arrange
            var collector = new AvroMetricsCollector();

            // Act
            collector.RecordCacheMiss("TestEntity", "Key");

            // Assert
            var snapshot = collector.GetSnapshot();

            // Counter should be recorded
            Assert.Contains(snapshot.Counters, kvp => kvp.Value.Name == "cache_misses");
            var counter = snapshot.Counters.First(kvp => kvp.Value.Name == "cache_misses").Value;
            Assert.Equal(1, counter.Value);
            Assert.Equal("TestEntity", counter.Tags["entity_type"]);
            Assert.Equal("Key", counter.Tags["serializer_type"]);
        }

        [Fact]
        public void RecordSerializationDuration_RecordsTimerAndHistogram()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var duration = TimeSpan.FromMilliseconds(150);

            // Act
            collector.RecordSerializationDuration("TestEntity", "Value", duration);

            // Assert
            var snapshot = collector.GetSnapshot();

            // Timer should be recorded
            Assert.Contains(snapshot.Timers, kvp => kvp.Value.Name == "serialization_duration");
            var timer = snapshot.Timers.First(kvp => kvp.Value.Name == "serialization_duration").Value;
            Assert.Equal(duration, timer.TotalTime);
            Assert.Equal("TestEntity", timer.Tags["entity_type"]);
        }

        [Fact]
        public void RecordSchemaRegistration_RecordsMultipleMetrics()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var duration = TimeSpan.FromMilliseconds(250);

            // Act
            collector.RecordSchemaRegistration("test-subject", true, duration);

            // Assert
            var snapshot = collector.GetSnapshot();

            // Counter should be recorded
            Assert.Contains(snapshot.Counters, kvp => kvp.Value.Name == "schema_registrations");
            var counter = snapshot.Counters.First(kvp => kvp.Value.Name == "schema_registrations").Value;
            Assert.Equal(1, counter.Value);
            Assert.Equal("test-subject", counter.Tags["subject"]);
            Assert.Equal(true, counter.Tags["success"]);

            // Timer should be recorded
            Assert.Contains(snapshot.Timers, kvp => kvp.Value.Name == "schema_registration_duration");
            var timer = snapshot.Timers.First(kvp => kvp.Value.Name == "schema_registration_duration").Value;
            Assert.Equal(duration, timer.TotalTime);
        }

        [Fact]
        public void UpdateGlobalMetrics_RecordsGauges()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var stats = new KsqlDsl.Avro.CacheStatistics
            {
                CachedItemCount = 150,
                HitRate = 0.85,
                TotalRequests = 1000,
                Uptime = TimeSpan.FromMinutes(30)
            };

            // Act
            collector.UpdateGlobalMetrics(stats);

            // Assert
            var snapshot = collector.GetSnapshot();

            Assert.Equal(4, snapshot.Gauges.Count);
            Assert.Contains(snapshot.Gauges, kvp => kvp.Value.Name == "cache_size" && (int)kvp.Value.Value == 150);
            Assert.Contains(snapshot.Gauges, kvp => kvp.Value.Name == "cache_hit_rate" && (double)kvp.Value.Value == 0.85);
            Assert.Contains(snapshot.Gauges, kvp => kvp.Value.Name == "total_requests" && (long)kvp.Value.Value == 1000);
            Assert.Contains(snapshot.Gauges, kvp => kvp.Value.Name == "uptime_minutes" && (double)kvp.Value.Value == 30.0);
        }

        [Fact]
        public void GetSnapshot_ReturnsCorrectData()
        {
            // Arrange
            var collector = new AvroMetricsCollector();

            collector.RecordCounter("counter1", 10);
            collector.RecordGauge("gauge1", 5.5);
            collector.RecordHistogram("histogram1", 100.0);
            collector.RecordTimer("timer1", TimeSpan.FromMilliseconds(500));

            // Act
            var snapshot = collector.GetSnapshot();

            // Assert
            Assert.Equal("Avro", snapshot.TargetTypeName);
            Assert.True(DateTime.UtcNow - snapshot.SnapshotTime < TimeSpan.FromSeconds(1));
            Assert.Single(snapshot.Counters);
            Assert.Single(snapshot.Gauges);
            Assert.Single(snapshot.Histograms);
            Assert.Single(snapshot.Timers);
        }

        [Fact]
        public void DifferentTags_CreateSeparateMetrics()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            var tags1 = new Dictionary<string, object?> { ["entity"] = "Entity1" };
            var tags2 = new Dictionary<string, object?> { ["entity"] = "Entity2" };

            // Act
            collector.RecordCounter("requests", 5, tags1);
            collector.RecordCounter("requests", 3, tags2);

            // Assert
            var snapshot = collector.GetSnapshot();
            Assert.Equal(2, snapshot.Counters.Count);

            Assert.Contains(snapshot.Counters.Values, c => c.Value == 5 && c.Tags["entity"].ToString() == "Entity1");
            Assert.Contains(snapshot.Counters.Values, c => c.Value == 3 && c.Tags["entity"].ToString() == "Entity2");
        }

        [Fact]
        public void Dispose_ClearsAllMetrics()
        {
            // Arrange
            var collector = new AvroMetricsCollector();
            collector.RecordCounter("test", 1);
            collector.RecordGauge("test", 1.0);

            // Act
            collector.Dispose();

            // Assert
            var snapshot = collector.GetSnapshot();
            Assert.Empty(snapshot.Counters);
            Assert.Empty(snapshot.Gauges);
            Assert.Empty(snapshot.Histograms);
            Assert.Empty(snapshot.Timers);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void RecordCounter_InvalidName_ThrowsException(string invalidName)
        {
            // Arrange
            var collector = new AvroMetricsCollector();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => collector.RecordCounter(invalidName, 1));
        }

        [Fact]
        public void RecordTimer_ZeroDuration_RecordsCorrectly()
        {
            // Arrange
            var collector = new AvroMetricsCollector();

            // Act
            collector.RecordTimer("zero_timer", TimeSpan.Zero);

            // Assert
            var snapshot = collector.GetSnapshot();
            var timer = snapshot.Timers.First().Value;
            Assert.Equal(TimeSpan.Zero, timer.TotalTime);
            Assert.Equal(TimeSpan.Zero, timer.MinTime);
            Assert.Equal(TimeSpan.Zero, timer.MaxTime);
            Assert.Equal(TimeSpan.Zero, timer.AverageTime);
        }
    }
}