using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KsqlDsl.Serialization.Abstractions
{
    public interface ISerializationManager<T> : IDisposable where T : class
    {
        Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default);
        Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default);
        Task<bool> ValidateRoundTripAsync(T entity, CancellationToken cancellationToken = default);
        SerializationStatistics GetStatistics();
        Type EntityType { get; }
        SerializationFormat Format { get; }
    }

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
        public long TotalSerializations;     // ✅ プロパティ → フィールド
        public long TotalDeserializations;   // ✅ プロパティ → フィールド
        public long CacheHits;               // ✅ プロパティ → フィールド
        public long CacheMisses;             // ✅ プロパティ → フィールド

    }


    public enum SerializationFormat
    {
        Avro,
        Json,
        Protobuf
    }
}