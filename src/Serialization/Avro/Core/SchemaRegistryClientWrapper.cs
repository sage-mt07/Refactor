using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Cache;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro.Core
{
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
            var registeredSchema = await _client.GetRegisteredSchemaAsync(subject, -1);
            return new AvroSchemaInfo
            {
                EntityType = typeof(object),
                Type = SerializerType.Value,
                SchemaId = registeredSchema.Id,
                Version = registeredSchema.Version,
                Subject = registeredSchema.Subject,
                AvroSchema = registeredSchema.SchemaString,
                RegisteredAt = DateTime.UtcNow,
                LastUsed = DateTime.UtcNow,
                UsageCount = 0
            };
        }

        public async Task<AvroSchemaInfo> GetSchemaByIdAsync(int schemaId)
        {
            var schema = await _client.GetSchemaAsync(schemaId);
            return new AvroSchemaInfo
            {
                EntityType = typeof(object),
                Type = SerializerType.Value,
                SchemaId = schemaId,
                Version = -1,  // 不明な場合のデフォルト値
                Subject = "unknown", // 不明な場合のデフォルト値
                AvroSchema = schema.SchemaString,
                RegisteredAt = DateTime.UtcNow,
                LastUsed = DateTime.UtcNow,
                UsageCount = 0
            };
        }

        public async Task<bool> CheckCompatibilityAsync(string subject, string avroSchema)
        {
            var schema = new ConfluentSchemaRegistry.Schema(avroSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            return await _client.IsCompatibleAsync(subject, schema);
        }

        public async Task<IList<int>> GetSchemaVersionsAsync(string subject)
        {
            return await _client.GetSubjectVersionsAsync(subject);
        }

        public async Task<AvroSchemaInfo> GetSchemaAsync(string subject, int version)
        {
            var registeredSchema = await _client.GetRegisteredSchemaAsync(subject, version);
            return new AvroSchemaInfo
            {
                EntityType = typeof(object),
                Type = SerializerType.Value,
                SchemaId = registeredSchema.Id,
                Version = registeredSchema.Version,
                Subject = registeredSchema.Subject,
                AvroSchema = registeredSchema.SchemaString,
                RegisteredAt = DateTime.UtcNow,
                LastUsed = DateTime.UtcNow,
                UsageCount = 0
            };
        }

        public async Task<IList<string>> GetAllSubjectsAsync()
        {
            return await _client.GetAllSubjectsAsync();
        }

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}