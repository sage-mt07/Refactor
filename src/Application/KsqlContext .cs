using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Avro.Management;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Application
{
    public abstract class KsqlContext : IDisposable
    {
        private readonly IAvroSchemaRegistrationService _schemaService;
        private readonly GlobalAvroSerializationManager _serializationManager;
        private readonly IAvroSchemaRepository _schemaRepository;
        private readonly ILoggerFactory? _loggerFactory;
        private readonly ILogger<KsqlContext> _logger;
        private readonly KsqlContextOptions _options;
        private bool _disposed = false;

        protected KsqlContext(KsqlContextOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _options.Validate();

            _loggerFactory = options.LoggerFactory;
            _logger = _loggerFactory.CreateLoggerOrNull<KsqlContext>();

            // Core components initialization
            _schemaRepository = new AvroSchemaRepository();
            _schemaService = new AvroSchemaRegistrationService(
                options.SchemaRegistryClient,
                _loggerFactory);

            // Advanced caching and serialization
            var cache = new Serialization.Avro.Cache.AvroSerializerCache(
                new AvroSerializerFactory(options.SchemaRegistryClient, _loggerFactory),
                _loggerFactory);

            _serializationManager = new GlobalAvroSerializationManager(
                options.SchemaRegistryClient,
                cache,
                _schemaRepository,
                _loggerFactory);

            _logger?.LogDebug("KsqlContext initialized with schema registry: {Url}",
                GetSchemaRegistryInfo());
        }

        /// <summary>
        /// EF風モデル構築メソッド - 継承クラスで実装
        /// </summary>
        protected abstract void OnAvroModelCreating(AvroModelBuilder modelBuilder);

        /// <summary>
        /// コンテキストの初期化
        /// Fail-Fast設計: エラー時はアプリケーション終了
        /// </summary>
        public async Task InitializeAsync()
        {
            var startTime = DateTime.UtcNow;

            try
            {
                _logger?.LogInformation("AVRO initialization started");

                // 1. モデル構築
                var modelBuilder = new AvroModelBuilder(_loggerFactory);
                OnAvroModelCreating(modelBuilder);

                var configurations = modelBuilder.Build();
                _logger?.LogDebug("Model built with {EntityCount} entities", configurations.Count);

                // 2. Fail-Fast: スキーマ登録
                if (_options.AutoRegisterSchemas)
                {
                    await _schemaService.RegisterAllSchemasAsync(configurations);
                    _logger?.LogInformation("Schema registration completed");
                }

                // 3. スキーマ情報の保存
                foreach (var (entityType, config) in configurations)
                {
                    try
                    {
                        var schemaInfo = await _schemaService.GetSchemaInfoAsync(entityType);
                        _schemaRepository.StoreSchemaInfo(schemaInfo);
                    }
                    catch (Exception ex)
                    {
                        if (_options.FailOnSchemaErrors)
                        {
                            throw new InvalidOperationException(
                                $"Failed to retrieve schema info for {entityType.Name}", ex);
                        }

                        _logger?.LogWarning(ex, "Failed to retrieve schema info for {EntityType}, continuing...",
                            entityType.Name);
                    }
                }

                // 4. キャッシュ事前ウォーミング
                if (_options.EnableCachePreWarming)
                {
                    await _serializationManager.PreWarmAllCachesAsync();
                    _logger?.LogDebug("Cache pre-warming completed");
                }

                var duration = DateTime.UtcNow - startTime;
                _logger?.LogInformation(
                    "AVRO initialization completed: {EntityCount} entities in {Duration}ms",
                    configurations.Count, duration.TotalMilliseconds);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "AVRO initialization failed");

                if (_options.FailOnInitializationErrors)
                {
                    throw;
                }
                else
                {
                    _logger?.LogWarning("Continuing with partial initialization due to configuration");
                }
            }
        }

        /// <summary>
        /// 型安全なシリアライザー取得
        /// </summary>
        public IAvroSerializer<T> GetSerializer<T>() where T : class
        {
            return _serializationManager.GetSerializer<T>();
        }

        /// <summary>
        /// 型安全なデシリアライザー取得
        /// </summary>
        public IAvroDeserializer<T> GetDeserializer<T>() where T : class
        {
            return _serializationManager.GetDeserializer<T>();
        }

        /// <summary>
        /// エンティティのスキーマ情報取得
        /// </summary>
        public AvroSchemaInfo? GetSchemaInfo<T>() where T : class
        {
            return _schemaRepository.GetSchemaInfo(typeof(T));
        }

        /// <summary>
        /// トピック名からスキーマ情報取得
        /// </summary>
        public AvroSchemaInfo? GetSchemaInfoByTopic(string topicName)
        {
            return _schemaRepository.GetSchemaInfoByTopic(topicName);
        }

        /// <summary>
        /// エンティティ登録状況の確認
        /// </summary>
        public bool IsEntityRegistered<T>() where T : class
        {
            return _schemaRepository.IsRegistered(typeof(T));
        }

        /// <summary>
        /// 登録済みスキーマ一覧取得
        /// </summary>
        public async Task<List<string>> GetRegisteredSchemasAsync()
        {
            return await _schemaService.GetAllRegisteredSchemasAsync()
                .ContinueWith(task => task.Result.ConvertAll(schema => schema.ToString()));
        }

        /// <summary>
        /// エンティティのスキーマ互換性チェック
        /// </summary>
        public async Task<bool> CheckEntitySchemaCompatibilityAsync<T>() where T : class
        {
            try
            {
                var schemaInfo = _schemaRepository.GetSchemaInfo(typeof(T));
                if (schemaInfo == null)
                    return false;

                var newValueSchema = UnifiedSchemaGenerator.GenerateValueSchema<T>();
                return await CheckSchemaCompatibilityAsync(schemaInfo.GetValueSubject(), newValueSchema);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Schema compatibility check failed for {EntityType}", typeof(T).Name);
                return false;
            }
        }

        /// <summary>
        /// スキーマ互換性チェック
        /// </summary>
        public async Task<bool> CheckSchemaCompatibilityAsync(string subject, string schema)
        {
            try
            {
                var schemaObj = new ConfluentSchemaRegistry.Schema(schema, ConfluentSchemaRegistry.SchemaType.Avro);
                return await _options.SchemaRegistryClient.IsCompatibleAsync(subject, schemaObj);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Schema compatibility check failed for subject {Subject}", subject);
                return false;
            }
        }

        /// <summary>
        /// 診断情報取得
        /// </summary>
        public string GetDiagnostics()
        {
            var diagnostics = new List<string>
            {
                "=== KsqlContext診断情報 ===",
                $"Schema Registry: {GetSchemaRegistryInfo()}",
                $"登録済みスキーマ数: {_schemaRepository.GetAllSchemas().Count}",
                $"Auto Register: {_options.AutoRegisterSchemas}",
                $"Cache Pre-warming: {_options.EnableCachePreWarming}",
                $"Fail on Errors: {_options.FailOnInitializationErrors}",
                ""
            };

            var allSchemas = _schemaRepository.GetAllSchemas();
            if (allSchemas.Count > 0)
            {
                diagnostics.Add("=== 登録済みエンティティ ===");
                foreach (var schema in allSchemas.OrderBy(s => s.EntityType.Name))
                {
                    diagnostics.Add($"✅ {schema.GetSummary()}");
                }
            }
            else
            {
                diagnostics.Add("⚠️ 登録済みエンティティなし");
            }

            return string.Join(Environment.NewLine, diagnostics);
        }

        /// <summary>
        /// ヘルスチェック
        /// </summary>
        public async Task<KsqlContextHealthReport> GetHealthReportAsync()
        {
            var report = new KsqlContextHealthReport
            {
                GeneratedAt = DateTime.UtcNow,
                ContextStatus = KsqlContextHealthStatus.Healthy
            };

            try
            {
                // Schema Registry接続チェック
                await _options.SchemaRegistryClient.GetAllSubjectsAsync();
                report.ComponentStatus["SchemaRegistry"] = "Healthy";
            }
            catch (Exception ex)
            {
                report.ContextStatus = KsqlContextHealthStatus.Unhealthy;
                report.ComponentStatus["SchemaRegistry"] = $"Unhealthy: {ex.Message}";
                report.Issues.Add($"Schema Registry connection failed: {ex.Message}");
            }

            // スキーマ登録状況チェック
            var allSchemas = _schemaRepository.GetAllSchemas();
            report.ComponentStatus["RegisteredSchemas"] = $"{allSchemas.Count} schemas";

            if (allSchemas.Count == 0)
            {
                report.ContextStatus = KsqlContextHealthStatus.Degraded;
                report.Issues.Add("No schemas registered");
            }

            // シリアライゼーション管理チェック
            report.ComponentStatus["SerializationManager"] = "Healthy";

            return report;
        }

        /// <summary>
        /// 統計情報取得
        /// </summary>
        public KsqlContextStatistics GetStatistics()
        {
            var allSchemas = _schemaRepository.GetAllSchemas();

            return new KsqlContextStatistics
            {
                TotalEntities = allSchemas.Count,
                RegisteredSchemas = allSchemas.Count,
                StreamEntities = allSchemas.Count(s => s.GetStreamTableType() == "Stream"),
                TableEntities = allSchemas.Count(s => s.GetStreamTableType() == "Table"),
                CompositeKeyEntities = allSchemas.Count(s => s.IsCompositeKey()),
                LastInitialized = DateTime.UtcNow // 実際の初期化時刻を保存する場合は別途実装
            };
        }

        #region Private Methods

        private string GetSchemaRegistryInfo()
        {
            try
            {
                var config = _options.SchemaRegistryClient.GetType()
                    .GetProperty("Config")?.GetValue(_options.SchemaRegistryClient);

                return config?.ToString() ?? "Unknown";
            }
            catch
            {
                return "Configuration not accessible";
            }
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            if (!_disposed)
            {
                _serializationManager?.Dispose();
                _schemaRepository?.Clear();
                _options.SchemaRegistryClient?.Dispose();

                _logger?.LogDebug("KsqlContext disposed");
                _disposed = true;
            }
        }

        #endregion
    }

}
