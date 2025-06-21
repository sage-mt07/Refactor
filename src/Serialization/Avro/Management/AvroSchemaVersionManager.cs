using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro.Management;

public class AvroSchemaVersionManager
{
    private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
    private readonly ILogger<AvroSchemaVersionManager>? _logger;

    public AvroSchemaVersionManager(
        ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
        ILoggerFactory? loggerFactory = null)
    {
        _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
        _logger = loggerFactory?.CreateLogger<AvroSchemaVersionManager>() ?? NullLogger<AvroSchemaVersionManager>.Instance;
    }

    /// <summary>
    /// スキーマアップグレード可能性チェック
    /// </summary>
    public async Task<bool> CanUpgradeAsync<T>(string newSchema) where T : class
    {
        try
        {
            var entityType = typeof(T);
            var topicName = GetTopicName(entityType);
            var subject = $"{topicName}-value";

            var schemaObj = new ConfluentSchemaRegistry.Schema(newSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            var isCompatible = await _schemaRegistryClient.IsCompatibleAsync(subject, schemaObj);

            _logger?.LogDebug("Schema upgrade compatibility check for {EntityType}: {Result}", entityType.Name, isCompatible);
            return isCompatible;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Schema upgrade compatibility check failed for {EntityType}", typeof(T).Name);
            return false;
        }
    }

    /// <summary>
    /// スキーマアップグレード実行
    /// </summary>
    public async Task<SchemaUpgradeResult> UpgradeAsync<T>() where T : class
    {
        try
        {
            var entityType = typeof(T);
            var topicName = GetTopicName(entityType);
            var newSchema = UnifiedSchemaGenerator.GenerateValueSchema<T>();

            var canUpgrade = await CanUpgradeAsync<T>(newSchema);
            if (!canUpgrade)
            {
                return new SchemaUpgradeResult
                {
                    Success = false,
                    Reason = "Schema is not compatible for upgrade"
                };
            }

            var subject = $"{topicName}-value";
            var schemaObj = new ConfluentSchemaRegistry.Schema(newSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            var newSchemaId = await _schemaRegistryClient.RegisterSchemaAsync(subject, schemaObj);

            _logger?.LogInformation("Schema upgraded successfully for {EntityType}: new ID {SchemaId}", entityType.Name, newSchemaId);

            return new SchemaUpgradeResult
            {
                Success = true,
                NewSchemaId = newSchemaId,
                Reason = "Schema upgraded successfully"
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

    /// <summary>
    /// 最新スキーマバージョン取得
    /// </summary>
    public async Task<int> GetLatestVersionAsync<T>() where T : class
    {
        try
        {
            var entityType = typeof(T);
            var topicName = GetTopicName(entityType);
            var subject = $"{topicName}-value";

            var latestSchema = await _schemaRegistryClient.GetRegisteredSchemaAsync(subject, -1);
            return latestSchema.Version;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to get latest schema version for {EntityType}", typeof(T).Name);
            return -1;
        }
    }

    /// <summary>
    /// スキーマバージョン履歴取得
    /// </summary>
    public async Task<System.Collections.Generic.List<int>> GetVersionHistoryAsync<T>() where T : class
    {
        try
        {
            var entityType = typeof(T);
            var topicName = GetTopicName(entityType);
            var subject = $"{topicName}-value";

            var versions = await _schemaRegistryClient.GetSubjectVersionsAsync(subject);
            return new System.Collections.Generic.List<int>(versions);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to get schema version history for {EntityType}", typeof(T).Name);
            return new System.Collections.Generic.List<int>();
        }
    }

    private string GetTopicName(Type entityType)
    {
        var topicAttribute = entityType.GetCustomAttribute<KsqlDsl.Core.Abstractions.TopicAttribute>();
        return topicAttribute?.TopicName ?? entityType.Name.ToLowerInvariant();
    }
}