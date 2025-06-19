using KsqlDsl.Serialization.Avro.Cache;
using KsqlDsl.Serialization.Avro.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro.Management
{
    public class SchemaVersionManager
    {
        // ✅ 修正: Confluent.SchemaRegistry.ISchemaRegistryClientを使用
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
        private readonly AvroSerializerCache _serializerCache;
        private readonly ILogger<SchemaVersionManager>? _logger;

        public SchemaVersionManager(
            ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
            AvroSerializerCache serializerCache,
            ILogger<SchemaVersionManager>? logger = null)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _serializerCache = serializerCache ?? throw new ArgumentNullException(nameof(serializerCache));
            _logger = logger;
        }

        public async Task<bool> CanUpgradeSchemaAsync<T>(string topicName) where T : class
        {
            var valueSubject = $"{topicName}-value";
            var currentSchema = await GetLatestSchemaAsync(valueSubject);

            if (currentSchema == null)
                return true;

            var newSchema = SchemaGenerator.GenerateSchema<T>();

            // ✅ 修正: IsCompatibleAsyncを使用
            var schemaObj = new ConfluentSchemaRegistry.Schema(newSchema, ConfluentSchemaRegistry.SchemaType.Avro);
            return await _schemaRegistryClient.IsCompatibleAsync(valueSubject, schemaObj);
        }

        public async Task<SchemaUpgradeResult> UpgradeSchemaAsync<T>(string topicName) where T : class
        {
            if (!await CanUpgradeSchemaAsync<T>(topicName))
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

                var keySchema = SchemaGenerator.GenerateKeySchema<T>();
                var valueSchema = SchemaGenerator.GenerateSchema<T>();

                // ✅ 修正: 新しいAPIを使用（Schema オブジェクトを作成）
                var keySchemaObj = new ConfluentSchemaRegistry.Schema(keySchema, ConfluentSchemaRegistry.SchemaType.Avro);
                var valueSchemaObj = new ConfluentSchemaRegistry.Schema(valueSchema, ConfluentSchemaRegistry.SchemaType.Avro);

                var keySchemaId = await _schemaRegistryClient.RegisterSchemaAsync(keySubject, keySchemaObj);
                var valueSchemaId = await _schemaRegistryClient.RegisterSchemaAsync(valueSubject, valueSchemaObj);

                _serializerCache.ClearCache<T>();

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

        public async Task<List<SchemaVersionInfo>> GetSchemaVersionHistoryAsync(string subject)
        {
            // ✅ 修正: GetSubjectVersionsAsyncを使用
            var versions = await _schemaRegistryClient.GetSubjectVersionsAsync(subject);
            var result = new List<SchemaVersionInfo>();

            foreach (var version in versions)
            {
                try
                {
                    // ✅ 修正: GetRegisteredSchemaAsyncを使用
                    var schema = await _schemaRegistryClient.GetRegisteredSchemaAsync(subject, version);
                    result.Add(new SchemaVersionInfo
                    {
                        Subject = subject,
                        Version = version,
                        SchemaId = schema.Id,
                        Schema = schema.SchemaString,
                        RegistrationTime = DateTime.UtcNow
                    });
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to retrieve schema version {Version} for subject {Subject}",
                        version, subject);
                }
            }

            return result.OrderBy(v => v.Version).ToList();
        }

        public async Task<SchemaCompatibilityReport> CheckCompatibilityAsync<T>(string topicName) where T : class
        {
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

                var keySchema = SchemaGenerator.GenerateKeySchema<T>();
                var valueSchema = SchemaGenerator.GenerateSchema<T>();

                // ✅ 修正: IsCompatibleAsyncを使用
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

        public async Task<List<string>> GetAllSubjectsForTopicAsync(string topicName)
        {
            var allSubjects = await _schemaRegistryClient.GetAllSubjectsAsync();
            return allSubjects.Where(s => s.StartsWith($"{topicName}-")).ToList();
        }

        public async Task<bool> DeleteSchemaVersionAsync(string subject, int version)
        {
            try
            {
                // ⚠️ 注意: Confluent.SchemaRegistry.ISchemaRegistryClient には削除メソッドがないため、
                // この機能は現在実装されていません。
                // 将来的にSDKが更新された場合や、REST APIを直接呼び出す実装に変更する場合のためのスタブです。

                _logger?.LogWarning("Schema deletion is not supported by the current Confluent Schema Registry .NET client");

                // TODO: REST API を直接呼び出して削除を実装する場合は、以下のようなHTTP呼び出しが必要:
                // DELETE /subjects/{subject}/versions/{version}
                // または DELETE /subjects/{subject}?permanent=true (全バージョン削除)
                await Task.CompletedTask;
                return false; // 現在は削除できないことを示す
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to delete schema version {Version} for subject {Subject}", version, subject);
                return false;
            }
        }

        private async Task<AvroSchemaInfo?> GetLatestSchemaAsync(string subject)
        {
            try
            {
                // ✅ 修正: GetRegisteredSchemaAsyncを使用
                var registeredSchema = await _schemaRegistryClient.GetRegisteredSchemaAsync(subject, -1);

                return new AvroSchemaInfo
                {
                    EntityType = typeof(object),
                    Type = SerializerType.Value,
                    SchemaId = registeredSchema.Id,
                    Subject = registeredSchema.Subject,
                    RegisteredAt = DateTime.UtcNow,
                    LastUsed = DateTime.UtcNow,
                    Version = registeredSchema.Version,
                    UsageCount = 0,
                    AvroSchema = registeredSchema.SchemaString
                };
            }
            catch
            {
                return null;
            }
        }
    }

    // ✅ 必要なクラスの定義（重複を避けるため、存在しない場合のみ定義）
    public class SchemaUpgradeResult
    {
        public bool Success { get; set; }
        public string? Reason { get; set; }
        public int? NewKeySchemaId { get; set; }
        public int? NewValueSchemaId { get; set; }
    }


}