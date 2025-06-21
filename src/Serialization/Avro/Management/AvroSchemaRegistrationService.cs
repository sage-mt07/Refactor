using KsqlDsl.Configuration;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro.Management
{
    public class AvroSchemaRegistrationService : IAvroSchemaRegistrationService
    {
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
        private readonly ILoggerFactory? _loggerFactory;
        private readonly ILogger<AvroSchemaRegistrationService> _logger;
        private readonly Dictionary<Type, AvroSchemaInfo> _registeredSchemas = new();

        public AvroSchemaRegistrationService(
            ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
            ILoggerFactory? loggerFactory = null)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLoggerOrNull<AvroSchemaRegistrationService>();
        }> _logger;
        private readonly Dictionary<Type, AvroSchemaInfo> _registeredSchemas = new();

        public AvroSchemaRegistrationService(
            ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
            ILogger<AvroSchemaRegistrationService> logger)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task RegisterAllSchemasAsync(IReadOnlyDictionary<Type, AvroEntityConfiguration> configurations)
        {
            var startTime = DateTime.UtcNow;
            var registrationTasks = new List<Task>();

            foreach (var (entityType, config) in configurations)
            {
                registrationTasks.Add(RegisterEntitySchemaAsync(entityType, config));
            }

            await Task.WhenAll(registrationTasks);

            var duration = DateTime.UtcNow - startTime;
            _logger.LogInformationWithLegacySupport(_loggerFactory, false,
                "AVRO schema registration completed: {Count} entities in {Duration}ms",
                configurations.Count, duration.TotalMilliseconds);
        }

        private async Task RegisterEntitySchemaAsync(Type entityType, AvroEntityConfiguration config)
        {
            try
            {
                var topicName = config.TopicName ?? entityType.Name;

                var keySchema = AvroSchemaGenerator.GenerateKeySchema(entityType, config);
                var valueSchema = AvroSchemaGenerator.GenerateValueSchema(entityType, config);

                var keySchemaId = await RegisterSchemaAsync($"{topicName}-key", keySchema);
                var valueSchemaId = await RegisterSchemaAsync($"{topicName}-value", valueSchema);

                _registeredSchemas[entityType] = new AvroSchemaInfo
                {
                    EntityType = entityType,
                    TopicName = topicName,
                    KeySchemaId = keySchemaId,
                    ValueSchemaId = valueSchemaId,
                    KeySchema = keySchema,
                    ValueSchema = valueSchema,
                    RegisteredAt = DateTime.UtcNow
                };

                _logger.LogDebugWithLegacySupport(_loggerFactory, false,
                    "Schema registered: {EntityType} → {Topic} (Key: {KeyId}, Value: {ValueId})",
                    entityType.Name, topicName, keySchemaId, valueSchemaId);
            }
            catch (Exception ex)
            {
                _logger.LogErrorWithLegacySupport(ex, _loggerFactory, false,
                    "Schema registration failed: {EntityType}", entityType.Name);
                throw new AvroSchemaRegistrationException($"Failed to register schema for {entityType.Name}", ex);
            }
        }

        private async Task<int> RegisterSchemaAsync(string subject, string avroSchema)
        {
            var schema = new ConfluentSchemaRegistry.Schema(avroSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            return await _schemaRegistryClient.RegisterSchemaAsync(subject, schema);
        }

        public async Task<AvroSchemaInfo> GetSchemaInfoAsync<T>() where T : class
        {
            var entityType = typeof(T);
            if (_registeredSchemas.TryGetValue(entityType, out var schemaInfo))
            {
                return schemaInfo;
            }
            throw new InvalidOperationException($"Schema not registered for type: {entityType.Name}");
        }

        public async Task<List<AvroSchemaInfo>> GetAllRegisteredSchemasAsync()
        {
            await Task.CompletedTask;
            return new List<AvroSchemaInfo>(_registeredSchemas.Values);
        }
    }
}