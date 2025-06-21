// src/Application/KsqlContext.cs - 簡素化・統合版
// 重複削除、未実装参照削除、Monitoring初期化除去済み

using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Cache;
using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Avro.Management;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Application
{
    public abstract class KsqlContext : IDisposable
    {
        private readonly IAvroSchemaRegistrationService _schemaService;
        private readonly AvroSerializerCache _cache;
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

            // Core components initialization（Tracing一切なし）
            _schemaRepository = new AvroSchemaRepository();
            _schemaService = new AvroSchemaRegistrationService(
                options.SchemaRegistryClient,
                _loggerFactory);

            // 簡素化：既存の実装を統合使用
            var factory = new AvroSerializerFactory(options.SchemaRegistryClient, _loggerFactory);
            _cache = new AvroSerializerCache(factory, _loggerFactory);

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
        /// Monitoring発動なし（初期化パス）
        /// </summary>
        public async Task InitializeAsync()
        {
            var startTime = DateTime.UtcNow;

            try
            {
                _logger?.LogInformation("AVRO initialization started");

                // 1. モデル構築（Tracing一切なし）
                var modelBuilder = new AvroModelBuilder(_loggerFactory);
                OnAvroModelCreating(modelBuilder);

                var configurations = modelBuilder.Build();
                _logger?.LogDebug("Model built with {EntityCount} entities", configurations.Count);

                // 2. Fail-Fast: スキーマ登録（初期化パス = Tracing無効）
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
                            _logger?.LogCritical(ex,
                                "FATAL: Schema info retrieval failed for {EntityType}. HUMAN INTERVENTION REQUIRED.",
                                entityType.Name);
                            throw new InvalidOperationException(
                                $"Failed to retrieve schema info for {entityType.Name}", ex);
                        }

                        _logger?.LogWarning(ex, "Failed to retrieve schema info for {EntityType}, continuing...",
                            entityType.Name);
                    }
                }

                // 4. キャッシュ事前ウォーミング（Tracing無効）
                if (_options.EnableCachePreWarming)
                {
                    var allSchemas = _schemaRepository.GetAllSchemas();
                    await _cache.PreWarmAsync(allSchemas);
                    _logger?.LogDebug("Cache pre-warming completed");
                }

                var duration = DateTime.UtcNow - startTime;
                _logger?.LogInformation(
                    "AVRO initialization completed: {EntityCount} entities in {Duration}ms",
                    configurations.Count, duration.TotalMilliseconds);
            }
            catch (Exception ex)
            {
                _logger?.LogCritical(ex,
                    "FATAL: AVRO initialization failed. HUMAN INTERVENTION REQUIRED.");

                if (_options.FailOnInitializationErrors)
                {
                    throw; // Fail Fast: 即座にアプリ終了
                }
                else
                {
                    _logger?.LogWarning("Continuing with partial initialization due to configuration");
                }
            }
        }

        /// <summary>
        /// 型安全なシリアライザー取得（運用パス = Tracing有効）
        /// </summary>
        public IAvroSerializer<T> GetSerializer<T>() where T : class
        {
            // TODO: 運用パスでのみTracingを有効化
            // using var activity = AvroActivitySource.StartOperation("get_serializer", typeof(T).Name);

            return _cache.GetOrCreateSerializer<T>();
        }

        /// <summary>
        /// 型安全なデシリアライザー取得（運用パス = Tracing有効）
        /// </summary>
        public IAvroDeserializer<T> GetDeserializer<T>() where T : class
        {
            // TODO: 運用パスでのみTracingを有効化
            // using var activity = AvroActivitySource.StartOperation("get_deserializer", typeof(T).Name);

            return _cache.GetOrCreateDeserializer<T>();
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
        /// 診断情報取得（簡素化版）
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
                _cache?.Dispose();
                _schemaRepository?.Clear();
                _options.SchemaRegistryClient?.Dispose();

                _logger?.LogDebug("KsqlContext disposed");
                _disposed = true;
            }
        }

        #endregion
    }
}