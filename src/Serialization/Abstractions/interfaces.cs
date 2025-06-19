using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Abstractions
{
    public interface IAvroSchemaProvider
    {
        Task<string> GetKeySchemaAsync<T>() where T : class;
        Task<string> GetValueSchemaAsync<T>() where T : class;
        Task<(string keySchema, string valueSchema)> GetSchemasAsync<T>() where T : class;
        Task<bool> ValidateSchemaAsync(string schema);
    }

    public class SerializerPair<T> where T : class
    {
        public ISerializer<object> KeySerializer { get; set; } = default!;
        public ISerializer<object> ValueSerializer { get; set; } = default!;
        public int KeySchemaId { get; set; }
        public int ValueSchemaId { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }

    public class DeserializerPair<T> where T : class
    {
        public IDeserializer<object> KeyDeserializer { get; set; } = default!;
        public IDeserializer<object> ValueDeserializer { get; set; } = default!;
        public int KeySchemaId { get; set; }
        public int ValueSchemaId { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }

    public class SerializationStatistics
    {
        public double HitRate => TotalSerializations > 0 ? (double)CacheHits / TotalSerializations : 0.0;
        public TimeSpan AverageLatency { get; set; }
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
        public long TotalSerializations;
        public long TotalDeserializations;
        public long CacheHits;
        public long CacheMisses;
    }

    public enum SerializationFormat
    {
        Avro,
        Json,
        Protobuf
    }
}