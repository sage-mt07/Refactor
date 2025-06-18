using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using KsqlDsl.Monitoring.Abstractions;
using KsqlDsl.Serialization.Avro.Cache;

namespace KsqlDsl.Monitoring.Metrics
{
    /// <summary>
    /// Avroシリアライザー向けメトリクス収集実装
    /// 設計理由：既存AvroMetricsからの責務移行、IMetricsCollector統合
    /// .NET Metricsとの統合により標準的な監視基盤との連携を実現
    /// </summary>
    public class AvroMetricsCollector : IMetricsCollector<object>, IDisposable
    {
        private static readonly Meter _meter = new("KsqlDsl.Monitoring.Avro", "1.0.0");

        // .NET Metrics統合
        private static readonly Counter<long> _cacheHitCounter =
            _meter.CreateCounter<long>("avro_cache_hits_total", description: "Total cache hits");
        private static readonly Counter<long> _cacheMissCounter =
            _meter.CreateCounter<long>("avro_cache_misses_total", description: "Total cache misses");
        private static readonly Counter<long> _schemaRegistrationCounter =
            _meter.CreateCounter<long>("avro_schema_registrations_total", description: "Total schema registrations");
        private static readonly Histogram<double> _serializationDuration =
            _meter.CreateHistogram<double>("avro_serialization_duration_ms", "ms", "Serialization duration");
        private static readonly Histogram<double> _schemaRegistrationDuration =
            _meter.CreateHistogram<double>("avro_schema_registration_duration_ms", "ms", "Schema registration duration");

        // 内部メトリクス管理
        private readonly ConcurrentDictionary<string, CounterMetric> _counters = new();
        private readonly ConcurrentDictionary<string, GaugeMetric> _gauges = new();
        private readonly ConcurrentDictionary<string, HistogramMetric> _histograms = new();
        private readonly ConcurrentDictionary<string, TimerMetric> _timers = new();

        private bool _disposed = false;

        public string TargetTypeName => "Avro";
        public DateTime StartTime { get; }

        public AvroMetricsCollector()
        {
            StartTime = DateTime.UtcNow;
        }

        public void RecordCounter(string name, long value, IDictionary<string, object?>? tags = null)
        {
            var key = BuildMetricKey(name, tags);
            _counters.AddOrUpdate(key,
                new CounterMetric
                {
                    Name = name,
                    Value = value,
                    LastUpdated = DateTime.UtcNow,
                    Tags = tags ?? new Dictionary<string, object?>()
                },
                (_, existing) =>
                {
                    existing.Value += value;
                    existing.LastUpdated = DateTime.UtcNow;
                    return existing;
                });
        }

        public void RecordGauge(string name, double value, IDictionary<string, object?>? tags = null)
        {
            var key = BuildMetricKey(name, tags);
            _gauges.AddOrUpdate(key,
                new GaugeMetric
                {
                    Name = name,
                    Value = value,
                    LastUpdated = DateTime.UtcNow,
                    Tags = tags ?? new Dictionary<string, object?>()
                },
                (_, existing) =>
                {
                    existing.Value = value;
                    existing.LastUpdated = DateTime.UtcNow;
                    return existing;
                });
        }

        public void RecordHistogram(string name, double value, IDictionary<string, object?>? tags = null)
        {
            var key = BuildMetricKey(name, tags);
            _histograms.AddOrUpdate(key,
                new HistogramMetric
                {
                    Name = name,
                    Count = 1,
                    Sum = value,
                    Min = value,
                    Max = value,
                    LastUpdated = DateTime.UtcNow,
                    Tags = tags ?? new Dictionary<string, object?>()
                },
                (_, existing) =>
                {
                    existing.Count++;
                    existing.Sum += value;
                    existing.Min = Math.Min(existing.Min, value);
                    existing.Max = Math.Max(existing.Max, value);
                    existing.LastUpdated = DateTime.UtcNow;
                    return existing;
                });
        }

        public void RecordTimer(string name, TimeSpan duration, IDictionary<string, object?>? tags = null)
        {
            var key = BuildMetricKey(name, tags);
            _timers.AddOrUpdate(key,
                new TimerMetric
                {
                    Name = name,
                    Count = 1,
                    TotalTime = duration,
                    MinTime = duration,
                    MaxTime = duration,
                    LastUpdated = DateTime.UtcNow,
                    Tags = tags ?? new Dictionary<string, object?>()
                },
                (_, existing) =>
                {
                    existing.Count++;
                    existing.TotalTime = existing.TotalTime.Add(duration);
                    existing.MinTime = existing.MinTime > duration ? duration : existing.MinTime;
                    existing.MaxTime = existing.MaxTime < duration ? duration : existing.MaxTime;
                    existing.LastUpdated = DateTime.UtcNow;
                    return existing;
                });
        }

        public MetricsSnapshot GetSnapshot()
        {
            return new MetricsSnapshot
            {
                TargetTypeName = TargetTypeName,
                SnapshotTime = DateTime.UtcNow,
                Counters = new Dictionary<string, CounterMetric>(_counters),
                Gauges = new Dictionary<string, GaugeMetric>(_gauges),
                Histograms = new Dictionary<string, HistogramMetric>(_histograms),
                Timers = new Dictionary<string, TimerMetric>(_timers)
            };
        }

        // 既存AvroMetricsとの互換性を保つ専用メソッド
        public void RecordCacheHit(string entityType, string serializerType)
        {
            var tags = new Dictionary<string, object?>
            {
                ["entity_type"] = entityType,
                ["serializer_type"] = serializerType
            };

            _cacheHitCounter.Add(1,
                new KeyValuePair<string, object?>("entity_type", entityType),
                new KeyValuePair<string, object?>("serializer_type", serializerType));

            RecordCounter("cache_hits", 1, tags);
        }

        public void RecordCacheMiss(string entityType, string serializerType)
        {
            var tags = new Dictionary<string, object?>
            {
                ["entity_type"] = entityType,
                ["serializer_type"] = serializerType
            };

            _cacheMissCounter.Add(1,
                new KeyValuePair<string, object?>("entity_type", entityType),
                new KeyValuePair<string, object?>("serializer_type", serializerType));

            RecordCounter("cache_misses", 1, tags);
        }

        public void RecordSerializationDuration(string entityType, string serializerType, TimeSpan duration)
        {
            var tags = new Dictionary<string, object?>
            {
                ["entity_type"] = entityType,
                ["serializer_type"] = serializerType
            };

            _serializationDuration.Record(duration.TotalMilliseconds,
                new KeyValuePair<string, object?>("entity_type", entityType),
                new KeyValuePair<string, object?>("serializer_type", serializerType));

            RecordTimer("serialization_duration", duration, tags);
        }

        public void RecordSchemaRegistration(string subject, bool success, TimeSpan duration)
        {
            var tags = new Dictionary<string, object?>
            {
                ["subject"] = subject,
                ["success"] = success
            };

            _schemaRegistrationCounter.Add(1,
                new KeyValuePair<string, object?>("subject", subject),
                new KeyValuePair<string, object?>("success", success));

            _schemaRegistrationDuration.Record(duration.TotalMilliseconds,
                new KeyValuePair<string, object?>("subject", subject),
                new KeyValuePair<string, object?>("success", success));

            RecordCounter("schema_registrations", 1, tags);
            RecordTimer("schema_registration_duration", duration, tags);
        }

        public void UpdateGlobalMetrics(CacheStatistics stats)
        {
            RecordGauge("cache_size", stats.CachedItemCount);
            RecordGauge("cache_hit_rate", stats.HitRate);
            RecordGauge("total_requests", stats.TotalRequests);
            RecordGauge("uptime_minutes", stats.Uptime.TotalMinutes);
        }

        /// <summary>
        /// Observable Metricsの登録（.NET Metrics統合）
        /// </summary>
        public void RegisterObservableMetrics(AvroSerializerCache cache)
        {
            _meter.CreateObservableGauge<int>("avro_cache_size", () => cache.GetCachedItemCount());
            _meter.CreateObservableGauge<double>("avro_cache_hit_rate", () => cache.GetGlobalStatistics().HitRate);
        }

        private string BuildMetricKey(string name, IDictionary<string, object?>? tags)
        {
            if (tags == null || !tags.Any())
                return name;

            var tagString = string.Join(",", tags.Select(kvp => $"{kvp.Key}={kvp.Value}"));
            return $"{name}|{tagString}";
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _meter?.Dispose();
                _counters.Clear();
                _gauges.Clear();
                _histograms.Clear();
                _timers.Clear();
                _disposed = true;
            }
        }
    }
}