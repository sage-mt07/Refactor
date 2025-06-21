using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Cache;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro
{
    public class AvroSerializationManager<T> : IAvroSerializationManager<T> where T : class
    {
        private readonly AvroSerializerCache _cache;
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
        private readonly ILogger<AvroSerializationManager<T>>? _logger;
        private bool _disposed = false;

        public Type EntityType => typeof(T);

        public AvroSerializationManager(
            ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
            AvroSerializerCache cache,
            ILogger<AvroSerializationManager<T>>? logger = null)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _logger = logger;
        }

        public async Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default)
        {
            var manager = _cache.GetAvroManager<T>();
            return await manager.GetSerializersAsync(cancellationToken);
        }

        public async Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default)
        {
            var manager = _cache.GetAvroManager<T>();
            return await manager.GetDeserializersAsync(cancellationToken);
        }

        public async Task<bool> ValidateRoundTripAsync(T entity, CancellationToken cancellationToken = default)
        {
            if (entity == null) return false;

            try
            {
                var manager = _cache.GetAvroManager<T>();
                return await manager.ValidateRoundTripAsync(entity, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Round-trip validation failed for {EntityType}", typeof(T).Name);
                return false;
            }
        }

        public SerializationStatistics GetStatistics()
        {
            var manager = _cache.GetAvroManager<T>();
            return manager.GetStatistics();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _cache?.Dispose();
                _disposed = true;
            }
        }
    }
}