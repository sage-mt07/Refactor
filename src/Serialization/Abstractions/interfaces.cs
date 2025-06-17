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

    public interface ISchemaVersionResolver
    {
        Task<int> ResolveKeySchemaVersionAsync<T>() where T : class;
        Task<int> ResolveValueSchemaVersionAsync<T>() where T : class;
        Task<bool> CanUpgradeAsync<T>(string newSchema) where T : class;
        Task<SchemaUpgradeResult> UpgradeAsync<T>() where T : class;
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
        public long TotalSerializations { get; set; }
        public long TotalDeserializations { get; set; }
        public long CacheHits { get; set; }
        public long CacheMisses { get; set; }
        public double HitRate => TotalSerializations > 0 ? (double)CacheHits / TotalSerializations : 0.0;
        public TimeSpan AverageLatency { get; set; }
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
    }

    public class SchemaUpgradeResult
    {
        public bool Success { get; set; }
        public string? Reason { get; set; }
        public int? NewKeySchemaId { get; set; }
        public int? NewValueSchemaId { get; set; }
        public DateTime UpgradedAt { get; set; } = DateTime.UtcNow;
    }

    public enum SerializationFormat
    {
        Avro,
        Json,
        Protobuf
    }
}