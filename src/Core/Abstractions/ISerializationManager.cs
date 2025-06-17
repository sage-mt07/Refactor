using System;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions
{
    /// <summary>
    /// シリアライザ共通インターフェース
    /// Avro/JSON/Protobuf対応
    /// </summary>
    public interface ISerializationManager<T> : IDisposable where T : class
    {
        Task<SerializerConfiguration<T>> GetConfigurationAsync();
        Task<bool> ValidateAsync(T entity);
        SerializationStatistics GetStatistics();

        Type EntityType { get; }
        SerializationFormat Format { get; }
    }

    public class SerializerConfiguration<T> where T : class
    {
        public object KeySerializer { get; set; } = default!;
        public object ValueSerializer { get; set; } = default!;
        public int KeySchemaId { get; set; }
        public int ValueSchemaId { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }

    public class SerializationStatistics
    {
        public long TotalOperations { get; set; }
        public long SuccessfulOperations { get; set; }
        public long FailedOperations { get; set; }
        public double SuccessRate => TotalOperations > 0 ? (double)SuccessfulOperations / TotalOperations : 0.0;
        public TimeSpan AverageLatency { get; set; }
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
    }

    public enum SerializationFormat
    {
        Avro,
        Json,
        Protobuf
    }
}