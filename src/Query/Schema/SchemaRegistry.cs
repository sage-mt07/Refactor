using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Query.Pipeline;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

namespace KsqlDsl.Query.Schema;

/// <summary>
/// スキーマレジストリ - エンティティモデルからksqlDBオブジェクトの自動生成・管理
/// 設計理由：query_redesign.mdの新アーキテクチャに準拠
/// </summary>
internal class SchemaRegistry : IDisposable
{
    private readonly DDLQueryGenerator _ddlGenerator;
    private readonly KsqlDbExecutor _executor;
    private readonly ConcurrentDictionary<Type, string> _registeredSchemas;
    private readonly ILogger _logger;
    private bool _disposed = false;

    public SchemaRegistry(KsqlDbExecutor executor, DDLQueryGenerator ddlGenerator, ILoggerFactory? loggerFactory = null)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _ddlGenerator = ddlGenerator ?? throw new ArgumentNullException(nameof(ddlGenerator));
        _registeredSchemas = new ConcurrentDictionary<Type, string>();
        _logger = loggerFactory.CreateLoggerOrNull<SchemaRegistry>();
    }

    public async Task RegisterSchemaAsync<T>(EntityModel entityModel) where T : class
    {
        var entityType = typeof(T);

        if (_registeredSchemas.ContainsKey(entityType))
        {
            _logger.LogDebug("Schema already registered for {EntityType}", entityType.Name);
            return; // Already registered
        }

        _logger.LogDebug("Registering schema for {EntityType}", entityType.Name);

        var topicName = entityModel.TopicAttribute?.TopicName ?? entityType.Name;
        var objectName = GenerateObjectName<T>();

        string createQuery;
        // EntityModelにStreamTableTypeがない場合の判定ロジック
        var streamTableType = DetermineStreamTableType<T>(entityModel);

        if (streamTableType == KsqlDsl.Query.Abstractions.StreamTableType.Stream)
        {
            createQuery = _ddlGenerator.GenerateCreateStream(objectName, topicName, entityModel);
            _logger.LogDebug("Generated CREATE STREAM for {EntityType}: {ObjectName}", entityType.Name, objectName);
        }
        else
        {
            createQuery = _ddlGenerator.GenerateCreateTable(objectName, topicName, entityModel);
            _logger.LogDebug("Generated CREATE TABLE for {EntityType}: {ObjectName}", entityType.Name, objectName);
        }

        try
        {
            await _executor.ExecuteDDLAsync(createQuery);
            _registeredSchemas.TryAdd(entityType, objectName);

            _logger.LogInformation("Successfully registered schema for {EntityType} as {ObjectName}",
                entityType.Name, objectName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to register schema for {EntityType}", entityType.Name);
            throw;
        }
    }

    public string GetObjectName<T>() where T : class
    {
        var entityType = typeof(T);

        if (_registeredSchemas.TryGetValue(entityType, out var name))
        {
            _logger.LogDebug("Retrieved object name for {EntityType}: {ObjectName}", entityType.Name, name);
            return name;
        }

        _logger.LogError("Schema not registered for {EntityType}", entityType.Name);
        throw new InvalidOperationException($"Schema not registered for {entityType.Name}");
    }

    public string GetObjectName(Type entityType)
    {
        if (_registeredSchemas.TryGetValue(entityType, out var name))
        {
            _logger.LogDebug("Retrieved object name for {EntityType}: {ObjectName}", entityType.Name, name);
            return name;
        }

        _logger.LogError("Schema not registered for {EntityType}", entityType.Name);
        throw new InvalidOperationException($"Schema not registered for {entityType.Name}");
    }

    public bool IsRegistered<T>() where T : class
    {
        var entityType = typeof(T);
        var isRegistered = _registeredSchemas.ContainsKey(entityType);

        _logger.LogDebug("Schema registration check for {EntityType}: {IsRegistered}", entityType.Name, isRegistered);
        return isRegistered;
    }

    public bool IsRegistered(Type entityType)
    {
        var isRegistered = _registeredSchemas.ContainsKey(entityType);

        _logger.LogDebug("Schema registration check for {EntityType}: {IsRegistered}", entityType.Name, isRegistered);
        return isRegistered;
    }

    public async Task UnregisterSchemaAsync<T>() where T : class
    {
        var entityType = typeof(T);

        if (!_registeredSchemas.TryRemove(entityType, out var objectName))
        {
            _logger.LogWarning("Attempted to unregister non-existent schema for {EntityType}", entityType.Name);
            return;
        }

        _logger.LogDebug("Unregistering schema for {EntityType}: {ObjectName}", entityType.Name, objectName);

        try
        {
            // Determine object type for DROP statement
            var dropQuery = $"DROP STREAM IF EXISTS {objectName}";
            await _executor.ExecuteDDLAsync(dropQuery);

            // Also try TABLE in case it was registered as a table
            dropQuery = $"DROP TABLE IF EXISTS {objectName}";
            await _executor.ExecuteDDLAsync(dropQuery);

            _logger.LogInformation("Successfully unregistered schema for {EntityType}: {ObjectName}",
                entityType.Name, objectName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to unregister schema for {EntityType}: {ObjectName}",
                entityType.Name, objectName);
            // Re-add to registry if drop failed
            _registeredSchemas.TryAdd(entityType, objectName);
            throw;
        }
    }

    public async Task UnregisterAllSchemasAsync()
    {
        _logger.LogInformation("Unregistering all schemas ({SchemaCount} schemas)", _registeredSchemas.Count);

        var tasks = new List<Task>();

        foreach (var kvp in _registeredSchemas)
        {
            var entityType = kvp.Key;
            var objectName = kvp.Value;

            tasks.Add(UnregisterSchemaInternalAsync(entityType, objectName));
        }

        try
        {
            await Task.WhenAll(tasks);
            _registeredSchemas.Clear();

            _logger.LogInformation("Successfully unregistered all schemas");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to unregister some schemas");
            throw;
        }
    }

    private async Task UnregisterSchemaInternalAsync(Type entityType, string objectName)
    {
        try
        {
            var dropStreamQuery = $"DROP STREAM IF EXISTS {objectName}";
            await _executor.ExecuteDDLAsync(dropStreamQuery);

            var dropTableQuery = $"DROP TABLE IF EXISTS {objectName}";
            await _executor.ExecuteDDLAsync(dropTableQuery);

            _logger.LogDebug("Unregistered schema for {EntityType}: {ObjectName}", entityType.Name, objectName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to unregister schema for {EntityType}: {ObjectName}",
                entityType.Name, objectName);
        }
    }

    private string GenerateObjectName<T>() where T : class
    {
        var entityName = typeof(T).Name;
        var objectName = $"{entityName.ToLower()}_base";

        _logger.LogDebug("Generated object name for {EntityType}: {ObjectName}", typeof(T).Name, objectName);
        return objectName;
    }

    /// <summary>
    /// EntityModelからStream/Table型を判定
    /// </summary>
    private KsqlDsl.Query.Abstractions.StreamTableType DetermineStreamTableType<T>(EntityModel entityModel) where T : class
    {
        var entityType = typeof(T);

        // 1. 明示的なksqlDB属性指定をチェック
        if (entityType.GetCustomAttribute<KsqlStreamAttribute>() != null)
        {
            _logger.LogDebug("Entity {EntityType} explicitly marked as STREAM", entityType.Name);
            return KsqlDsl.Query.Abstractions.StreamTableType.Stream;
        }

        if (entityType.GetCustomAttribute<KsqlTableAttribute>() != null)
        {
            _logger.LogDebug("Entity {EntityType} explicitly marked as TABLE", entityType.Name);
            return KsqlDsl.Query.Abstractions.StreamTableType.Table;
        }

        // 2. EF Core TableAttribute の存在確認
        if (entityType.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.TableAttribute>() != null)
        {
            _logger.LogDebug("Entity {EntityType} has EF Core [Table] attribute, defaulting to TABLE", entityType.Name);
            return KsqlDsl.Query.Abstractions.StreamTableType.Table;
        }

        // 3. キープロパティの有無で判定
        if (entityModel.KeyProperties != null && entityModel.KeyProperties.Length > 0)
        {
            _logger.LogDebug("Entity {EntityType} has key properties, defaulting to TABLE", entityType.Name);
            return KsqlDsl.Query.Abstractions.StreamTableType.Table;
        }

        // 4. デフォルトはStream
        _logger.LogDebug("Entity {EntityType} defaulting to STREAM", entityType.Name);
        return KsqlDsl.Query.Abstractions.StreamTableType.Stream;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _logger.LogDebug("Disposing SchemaRegistry");

            try
            {
                // Synchronously unregister all schemas on dispose
                UnregisterAllSchemasAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during SchemaRegistry disposal");
            }

            _disposed = true;
            _logger.LogDebug("SchemaRegistry disposed");
        }
    }
}