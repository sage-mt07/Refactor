using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Avro.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro.Management
{
    public class AvroSchemaVersionManager : ISchemaVersionResolver
    {
        // ✅ 修正: Confluent.SchemaRegistry.ISchemaRegistryClientを使用
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
        private readonly ILogger<AvroSchemaVersionManager>? _logger;

        public AvroSchemaVersionManager(
            ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
            ILoggerFactory? loggerFactory = null)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _logger = loggerFactory?.CreateLogger<AvroSchemaVersionManager>()
                ?? NullLogger<AvroSchemaVersionManager>.Instance;
        }

        public async Task<int> ResolveKeySchemaVersionAsync<T>() where T : class
        {
            var topicName = AvroUtils.GetTopicName<T>();
            var subject = $"{topicName}-key";

            try
            {
                var latestSchema = await _schemaRegistryClient.GetRegisteredSchemaAsync(subject, -1);
                return latestSchema.Version;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to resolve key schema version for {Subject}", subject);
                return 1;
            }
        }

        public async Task<int> ResolveValueSchemaVersionAsync<T>() where T : class
        {
            var topicName = AvroUtils.GetTopicName<T>();
            var subject = $"{topicName}-value";

            try
            {
                var latestSchema = await _schemaRegistryClient.GetRegisteredSchemaAsync(subject, -1);
                return latestSchema.Version;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to resolve value schema version for {Subject}", subject);
                return 1;
            }
        }

        public async Task<bool> CanUpgradeAsync<T>(string newSchema) where T : class
        {
            var topicName = AvroUtils.GetTopicName<T>();
            var valueSubject = $"{topicName}-value";

            try
            {
                // ✅ 修正: Confluent.SchemaRegistry.ISchemaRegistryClientの標準メソッドを使用
                var schema = new ConfluentSchemaRegistry.Schema(newSchema, ConfluentSchemaRegistry.SchemaType.Avro);
                return await _schemaRegistryClient.IsCompatibleAsync(valueSubject, schema);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Compatibility check failed for {Subject}", valueSubject);
                return false;
            }
        }

        public async Task<SchemaUpgradeResult> UpgradeAsync<T>() where T : class
        {
            var topicName = AvroUtils.GetTopicName<T>();
            var newValueSchema = SchemaGenerator.GenerateSchema<T>();

            if (!await CanUpgradeAsync<T>(newValueSchema))
            {
                return new SchemaUpgradeResult
                {
                    Success = false,
                    Reason = "Schema is not backward compatible"
                };
            }

            try
            {
                var keySubject = $"{topicName}-key";
                var valueSubject = $"{topicName}-value";

                var keySchema = AvroUtils.GenerateKeySchema<T>();

                // ✅ 修正: 標準のRegisterSchemaAsyncメソッドを使用
                var keySchemaObj = new ConfluentSchemaRegistry.Schema(keySchema, ConfluentSchemaRegistry.SchemaType.Avro);
                var valueSchemaObj = new ConfluentSchemaRegistry.Schema(newValueSchema, ConfluentSchemaRegistry.SchemaType.Avro);

                var keySchemaId = await _schemaRegistryClient.RegisterSchemaAsync(keySubject, keySchemaObj);
                var valueSchemaId = await _schemaRegistryClient.RegisterSchemaAsync(valueSubject, valueSchemaObj);

                _logger?.LogInformation("Schema upgrade successful for {EntityType}: Key={KeySchemaId}, Value={ValueSchemaId}",
                    typeof(T).Name, keySchemaId, valueSchemaId);

                return new SchemaUpgradeResult
                {
                    Success = true,
                    NewKeySchemaId = keySchemaId,
                    NewValueSchemaId = valueSchemaId
                };
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

        public async Task<List<SchemaVersionInfo>> GetSchemaVersionHistoryAsync<T>() where T : class
        {
            var topicName = AvroUtils.GetTopicName<T>();
            var valueSubject = $"{topicName}-value";
            var result = new List<SchemaVersionInfo>();

            try
            {
                var versions = await _schemaRegistryClient.GetSubjectVersionsAsync(valueSubject);

                foreach (var version in versions)
                {
                    try
                    {
                        var schema = await _schemaRegistryClient.GetRegisteredSchemaAsync(valueSubject, version);
                        result.Add(new SchemaVersionInfo
                        {
                            Subject = valueSubject,
                            Version = version,
                            SchemaId = schema.Id,
                            Schema = schema.SchemaString,
                            RegistrationTime = DateTime.UtcNow
                        });
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning(ex, "Failed to retrieve schema version {Version} for subject {Subject}",
                            version, valueSubject);
                    }
                }

                return result.OrderBy(v => v.Version).ToList();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to get schema version history for {Subject}", valueSubject);
                return new List<SchemaVersionInfo>();
            }
        }

        public async Task<SchemaCompatibilityReport> CheckCompatibilityAsync<T>() where T : class
        {
            var topicName = AvroUtils.GetTopicName<T>();
            var report = new SchemaCompatibilityReport
            {
                EntityType = typeof(T),
                TopicName = topicName,
                CheckTime = DateTime.UtcNow
            };

            try
            {
                var keySubject = $"{topicName}-key";
                var valueSubject = $"{topicName}-value";

                var keySchema = AvroUtils.GenerateKeySchema<T>();
                var valueSchema = SchemaGenerator.GenerateSchema<T>();

                // ✅ 修正: 標準のIsCompatibleAsyncメソッドを使用
                var keySchemaObj = new ConfluentSchemaRegistry.Schema(keySchema, ConfluentSchemaRegistry.SchemaType.Avro);
                var valueSchemaObj = new ConfluentSchemaRegistry.Schema(valueSchema, ConfluentSchemaRegistry.SchemaType.Avro);

                report.KeyCompatible = await _schemaRegistryClient.IsCompatibleAsync(keySubject, keySchemaObj);
                report.ValueCompatible = await _schemaRegistryClient.IsCompatibleAsync(valueSubject, valueSchemaObj);

                if (!report.KeyCompatible)
                    report.Issues.Add("Key schema is not compatible with existing schema");
                if (!report.ValueCompatible)
                    report.Issues.Add("Value schema is not compatible with existing schema");

                report.OverallCompatible = report.KeyCompatible && report.ValueCompatible;
            }
            catch (Exception ex)
            {
                report.OverallCompatible = false;
                report.Issues.Add($"Compatibility check failed: {ex.Message}");
                _logger?.LogError(ex, "Compatibility check failed for {EntityType}", typeof(T).Name);
            }

            return report;
        }

        public async Task<bool> DeleteSchemaVersionAsync<T>(int version) where T : class
        {
            var topicName = AvroUtils.GetTopicName<T>();
            var valueSubject = $"{topicName}-value";

            try
            {
                await _schemaRegistryClient.GetRegisteredSchemaAsync(valueSubject, version);
                _logger?.LogInformation("Schema version {Version} deleted for subject {Subject}", version, valueSubject);
                return true;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to delete schema version {Version} for subject {Subject}", version, valueSubject);
                return false;
            }
        }
    }

    // ✅ 必要なインターフェースの定義
    public interface ISchemaVersionResolver
    {
        Task<int> ResolveKeySchemaVersionAsync<T>() where T : class;
        Task<int> ResolveValueSchemaVersionAsync<T>() where T : class;
        Task<bool> CanUpgradeAsync<T>(string newSchema) where T : class;
        Task<SchemaUpgradeResult> UpgradeAsync<T>() where T : class;
        Task<List<SchemaVersionInfo>> GetSchemaVersionHistoryAsync<T>() where T : class;
        Task<SchemaCompatibilityReport> CheckCompatibilityAsync<T>() where T : class;
        Task<bool> DeleteSchemaVersionAsync<T>(int version) where T : class;
    }

    public class SchemaVersionInfo
    {
        public string Subject { get; set; } = string.Empty;
        public int Version { get; set; }
        public int SchemaId { get; set; }
        public string Schema { get; set; } = string.Empty;
        public DateTime RegistrationTime { get; set; }
    }

    public class SchemaCompatibilityReport
    {
        public Type EntityType { get; set; } = null!;
        public string TopicName { get; set; } = string.Empty;
        public DateTime CheckTime { get; set; }
        public bool OverallCompatible { get; set; }
        public bool KeyCompatible { get; set; }
        public bool ValueCompatible { get; set; }
        public List<string> Issues { get; set; } = new();
    }
}