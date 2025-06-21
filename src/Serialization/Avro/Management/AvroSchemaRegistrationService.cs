// src/Serialization/Avro/Management/AvroSchemaRegistrationService.cs
// インターフェース準拠修正版

using KsqlDsl.Core.Extensions;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Avro.Exceptions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
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
            _logger = _loggerFactory.CreateLoggerOrNull<AvroSchemaRegistrationService>();
        }

        /// <summary>
        /// 全スキーマ登録（初期化パス = Monitoring無効）
        /// </summary>
        public async Task RegisterAllSchemasAsync(IReadOnlyDictionary<Type, AvroEntityConfiguration> configurations)
        {
            var startTime = DateTime.UtcNow;
            var registrationTasks = new List<Task>();

            _logger?.LogInformation("Schema registration started: {EntityCount} entities", configurations.Count);

            foreach (var (entityType, config) in configurations)
            {
                registrationTasks.Add(RegisterEntitySchemaAsync(entityType, config));
            }

            await Task.WhenAll(registrationTasks);

            var duration = DateTime.UtcNow - startTime;
            _logger?.LogInformation(
                "AVRO schema registration completed: {Count} entities in {Duration}ms",
                configurations.Count, duration.TotalMilliseconds);
        }

        /// <summary>
        /// エンティティスキーマ登録（Tracing無効）
        /// </summary>
        private async Task RegisterEntitySchemaAsync(Type entityType, AvroEntityConfiguration config)
        {
            try
            {
                var topicName = config.GetEffectiveTopicName();

                // スキーマ生成（統一実装使用）
                var keySchema = UnifiedSchemaGenerator.GenerateKeySchema(config);
                var valueSchema = UnifiedSchemaGenerator.GenerateValueSchema(entityType, config);

                // Schema Registry登録（Retry機能は上位で実装）
                var keySchemaId = await RegisterSchemaAsync($"{topicName}-key", keySchema);
                var valueSchemaId = await RegisterSchemaAsync($"{topicName}-value", valueSchema);

                // 登録結果を保存
                _registeredSchemas[entityType] = new AvroSchemaInfo
                {
                    EntityType = entityType,
                    TopicName = topicName,
                    KeySchemaId = keySchemaId,
                    ValueSchemaId = valueSchemaId,
                    KeySchema = keySchema,
                    ValueSchema = valueSchema,
                    RegisteredAt = DateTime.UtcNow,
                    KeyProperties = config.KeyProperties
                };

                _logger?.LogDebug("Schema registered: {EntityType} → {Topic} (Key: {KeyId}, Value: {ValueId})",
                    entityType.Name, topicName, keySchemaId, valueSchemaId);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Schema registration failed: {EntityType}", entityType.Name);
                throw new AvroSchemaRegistrationException($"Failed to register schema for {entityType.Name}", ex);
            }
        }

        /// <summary>
        /// スキーマ登録（基本実装）
        /// </summary>
        private async Task<int> RegisterSchemaAsync(string subject, string avroSchema)
        {
            var schema = new ConfluentSchemaRegistry.Schema(avroSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            return await _schemaRegistryClient.RegisterSchemaAsync(subject, schema);
        }

        /// <summary>
        /// 型指定でのスキーマ情報取得
        /// </summary>
        public async Task<AvroSchemaInfo> GetSchemaInfoAsync<T>() where T : class
        {
            await Task.CompletedTask;
            return GetSchemaInfoAsync(typeof(T));
        }

        /// <summary>
        /// Type指定でのスキーマ情報取得
        /// </summary>
        public AvroSchemaInfo GetSchemaInfoAsync(Type entityType)
        {
            if (_registeredSchemas.TryGetValue(entityType, out var schemaInfo))
            {
                return schemaInfo;
            }
            throw new InvalidOperationException($"Schema not registered for type: {entityType.Name}");
        }

        /// <summary>
        /// 全登録スキーマ取得
        /// </summary>
        public async Task<List<AvroSchemaInfo>> GetAllRegisteredSchemasAsync()
        {
            await Task.CompletedTask;
            return new List<AvroSchemaInfo>(_registeredSchemas.Values);
        }
    }
}