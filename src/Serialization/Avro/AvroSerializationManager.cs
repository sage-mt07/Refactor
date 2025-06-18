using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Cache;
using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Avro.Management;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro
{
    public class AvroSerializationManager<T> : ISerializationManager<T> where T : class
    {
        private readonly AvroSerializerCache _cache;
        private readonly AvroSchemaVersionManager _versionManager;
        private readonly AvroSchemaBuilder _schemaBuilder;
        private readonly ILogger<AvroSerializationManager<T>>? _logger;
        private bool _disposed = false;

        public Type EntityType => typeof(T);
        public SerializationFormat Format => SerializationFormat.Avro;

        public AvroSerializationManager(
            ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
            ILoggerFactory? logger = null)
        {
            var factory = new AvroSerializerFactory(schemaRegistryClient, logger);
            _cache = new AvroSerializerCache(factory, logger);
            _versionManager = new AvroSchemaVersionManager(
                new SchemaRegistryClientWrapper(schemaRegistryClient), logger);
            _schemaBuilder = new AvroSchemaBuilder();

            _logger = logger?.CreateLogger<AvroSerializationManager<T>>()
                     ?? NullLogger<AvroSerializationManager<T>>.Instance;


        }

        public async Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default)
        {
            var manager = _cache.GetManager<T>();
            return await manager.GetSerializersAsync(cancellationToken);
        }

        public async Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default)
        {
            var manager = _cache.GetManager<T>();
            return await manager.GetDeserializersAsync(cancellationToken);
        }

        public async Task<bool> ValidateRoundTripAsync(T entity, CancellationToken cancellationToken = default)
        {
            if (entity == null)
                return false;

            try
            {
                var manager = _cache.GetManager<T>();
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
            var manager = _cache.GetManager<T>();
            return manager.GetStatistics();
        }

        public async Task<bool> CanUpgradeSchemaAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                var newSchema = await _schemaBuilder.GetValueSchemaAsync<T>();
                return await _versionManager.CanUpgradeAsync<T>(newSchema);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Schema upgrade check failed for {EntityType}", typeof(T).Name);
                return false;
            }
        }

        public async Task<SchemaUpgradeResult> UpgradeSchemaAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                var result = await _versionManager.UpgradeAsync<T>();

                if (result.Success)
                {
                    _cache.ClearCache<T>();
                }

                return result;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Schema upgrade failed for {EntityType}", typeof(T).Name);
                return new SchemaUpgradeResult
                {
                    Success = false,
                    Reason = ex.Message
                };
            }
        }

        public async Task<(string keySchema, string valueSchema)> GetCurrentSchemasAsync()
        {
            return await _schemaBuilder.GetSchemasAsync<T>();
        }

        public void ClearCache()
        {
            _cache.ClearCache<T>();
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

    internal class SchemaRegistryClientWrapper : ISchemaRegistryClient
    {
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _client;

        public SchemaRegistryClientWrapper(ConfluentSchemaRegistry.ISchemaRegistryClient client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public async Task<(int keySchemaId, int valueSchemaId)> RegisterTopicSchemasAsync(string topicName, string keySchema, string valueSchema)
        {
            var keySchemaId = await RegisterKeySchemaAsync(topicName, keySchema);
            var valueSchemaId = await RegisterValueSchemaAsync(topicName, valueSchema);
            return (keySchemaId, valueSchemaId);
        }

        public async Task<int> RegisterKeySchemaAsync(string topicName, string keySchema)
        {
            var subject = $"{topicName}-key";
            var schema = new ConfluentSchemaRegistry.Schema(keySchema, ConfluentSchemaRegistry.SchemaType.Avro);
            return await _client.RegisterSchemaAsync(subject, schema);
        }

        public async Task<int> RegisterValueSchemaAsync(string topicName, string valueSchema)
        {
            var subject = $"{topicName}-value";
            var schema = new ConfluentSchemaRegistry.Schema(valueSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            return await _client.RegisterSchemaAsync(subject, schema);
        }

        public async Task<int> RegisterSchemaAsync(string subject, string avroSchema)
        {
            var schema = new ConfluentSchemaRegistry.Schema(avroSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            return await _client.RegisterSchemaAsync(subject, schema);
        }

        public async Task<AvroSchemaInfo> GetLatestSchemaAsync(string subject)
        {
            var schema = await _client.GetLatestSchemaAsync(subject);
            return new AvroSchemaInfo
            {
                SchemaId = schema.Id,
                Version = schema.Version,
                Subject = schema.Subject,
                AvroSchema = schema.SchemaString
            };
        }

        public async Task<AvroSchemaInfo> GetSchemaByIdAsync(int schemaId)
        {
            var schema = await _client.GetSchemaAsync(schemaId);
            return new AvroSchemaInfo
            {
                SchemaId = schema.Id,
                Version = schema.Version,
                Subject = schema.Subject,
                AvroSchema = schema.SchemaString
            };
        }

        public async Task<bool> CheckCompatibilityAsync(string subject, string avroSchema)
        {
            var schema = new ConfluentSchemaRegistry.Schema(avroSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            var results = await _client.IsCompatibleAsync(subject, schema);
            return results;
        }

        public async Task<System.Collections.Generic.IList<int>> GetSchemaVersionsAsync(string subject)
        {
            return await _client.GetSubjectVersionsAsync(subject);
        }

        public async Task<AvroSchemaInfo> GetSchemaAsync(string subject, int version)
        {
            var schema = await _client.GetSchemaAsync(subject, version);
            return new AvroSchemaInfo
            {
                Id = schema.Id,
                Version = schema.Version,
                Subject = schema.Subject,
                AvroSchema = schema.SchemaString
            };
        }

        public async Task<System.Collections.Generic.IList<string>> GetAllSubjectsAsync()
        {
            return await _client.GetSubjectsAsync();
        }

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}