using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace KsqlDsl.Serialization.Avro.Metrics
{
    /// <summary>
    /// Avro操作のメトリクス収集（Serialization専用）
    /// </summary>
    public class AvroMetricsCollector
    {
        private readonly ConcurrentDictionary<string, long> _counters = new();
        private readonly ConcurrentDictionary<string, TimeSpan> _durations = new();

        /// <summary>
        /// シリアライゼーション時間を記録
        /// </summary>
        public void RecordSerializationDuration(string entityTypeName, string serializerType, TimeSpan duration)
        {
            var key = $"{entityTypeName}_{serializerType}_duration";
            _durations.AddOrUpdate(key, duration, (k, v) => TimeSpan.FromTicks((v.Ticks + duration.Ticks) / 2));

            var countKey = $"{entityTypeName}_{serializerType}_count";
            _counters.AddOrUpdate(countKey, 1, (k, v) => v + 1);
        }

        /// <summary>
        /// スキーマ登録を記録
        /// </summary>
        public void RecordSchemaRegistration(string entityTypeName, bool success, TimeSpan duration)
        {
            var key = success ? $"{entityTypeName}_schema_success" : $"{entityTypeName}_schema_failure";
            _counters.AddOrUpdate(key, 1, (k, v) => v + 1);
        }

        /// <summary>
        /// 統計情報を取得
        /// </summary>
        public AvroSerializationStatistics GetStatistics()
        {
            return new AvroSerializationStatistics
            {
                Counters = new Dictionary<string, long>(_counters),
                Durations = new Dictionary<string, TimeSpan>(_durations),
                LastUpdated = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// Avroシリアライゼーション統計
    /// </summary>
    public class AvroSerializationStatistics
    {
        public Dictionary<string, long> Counters { get; set; } = new();
        public Dictionary<string, TimeSpan> Durations { get; set; } = new();
        public DateTime LastUpdated { get; set; }
    }
}


