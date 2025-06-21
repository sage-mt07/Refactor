using KsqlDsl.Serialization.Avro.Cache;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro.Core;

internal class SchemaRegistryClientWrapper : IDisposable
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





    public async Task<bool> CheckCompatibilityAsync(string subject, string avroSchema)
    {
        var schema = new ConfluentSchemaRegistry.Schema(avroSchema, ConfluentSchemaRegistry.SchemaType.Avro);
        return await _client.IsCompatibleAsync(subject, schema);
    }

    public async Task<IList<int>> GetSchemaVersionsAsync(string subject)
    {
        return await _client.GetSubjectVersionsAsync(subject);
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