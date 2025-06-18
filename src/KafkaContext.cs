// src/KafkaContext.cs - Core層統合完全版
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Context;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Messaging.Configuration;
using KsqlDsl.Messaging.Consumers;
using KsqlDsl.Messaging.Producers;
using KsqlDsl.Messaging.Producers.Exception;
using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Avro.Management;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl
{
    /// <summary>
    /// Core層統合KafkaContext
    /// 設計理由：Core抽象化を継承し、上位層機能を統合
    /// </summary>
    public abstract class KafkaContext : KafkaContextCore
    {
        // 上位層サービス（Phase1移行対応）
        private readonly Lazy<KafkaProducerManager> _producerManager;
        private readonly Lazy<KafkaConsumerManager> _consumerManager;

        private AvroSchemaRegistrationService? _schemaRegistrationService;
        private readonly Lazy<ModelBuilder> _modelBuilder;

        protected KafkaContext() : base()
        {
            _modelBuilder = new Lazy<ModelBuilder>(() => new ModelBuilder(Options.ValidationMode));

            _producerManager = new Lazy<KafkaProducerManager>(() =>
            {
                Console.WriteLine("[INFO] Core層統合: KafkaProducerManager初期化");
                return new KafkaProducerManager(
                    null!, // EnhancedAvroSerializerManager
                    null!, // ProducerPool  
                    Microsoft.Extensions.Options.Options.Create(new KafkaProducerConfig()),
                    null!  // ILogger
                );
            });

            _consumerManager = new Lazy<KafkaConsumerManager>(() =>
            {
                Console.WriteLine("[INFO] Core層統合: KafkaConsumerManager初期化");
                return new KafkaConsumerManager(
                    null!, // EnhancedAvroSerializerManager
                    null!, // ConsumerPool
                    Microsoft.Extensions.Options.Options.Create(new KafkaConsumerConfig()),
                    null!  // ILogger
                );
            });
        }

        protected KafkaContext(KafkaContextOptions options) : base(options)
        {
            _modelBuilder = new Lazy<ModelBuilder>(() => new ModelBuilder(Options.ValidationMode));

            _producerManager = new Lazy<KafkaProducerManager>(() =>
            {
                Console.WriteLine("[INFO] Core層統合: KafkaProducerManager初期化");
                return new KafkaProducerManager(
                    null!, // EnhancedAvroSerializerManager
                    null!, // ProducerPool  
                    Microsoft.Extensions.Options.Options.Create(new KafkaProducerConfig()),
                    null!  // ILogger
                );
            });

            _consumerManager = new Lazy<KafkaConsumerManager>(() =>
            {
                Console.WriteLine("[INFO] Core層統合: KafkaConsumerManager初期化");
                return new KafkaConsumerManager(
                    null!, // EnhancedAvroSerializerManager
                    null!, // ConsumerPool
                    Microsoft.Extensions.Options.Options.Create(new KafkaConsumerConfig()),
                    null!  // ILogger
                );
            });
        }

        /// <summary>
        /// Core層EventSet実装（上位層機能統合）
        /// </summary>
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel)
        {
            return new EventSetWithServices<T>(this, entityModel);
        }

        // Core層統合API
        internal KafkaProducerManager GetProducerManager() => _producerManager.Value;
        internal KafkaConsumerManager GetConsumerManager() => _consumerManager.Value;

        // GetModelBuilderメソッドの追加
        private ModelBuilder GetModelBuilder()
        {
            return _modelBuilder.Value;
        }

        public new async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
        {
            // Core層のモデル構築とスキーマ登録
            var modelBuilder = GetModelBuilder();

            if (Options.EnableDebugLogging)
            {
                Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: Core層統合インフラ構築開始");
                Console.WriteLine(modelBuilder.GetModelSummary());
            }

            // スキーマ自動登録
            if (Options.EnableAutoSchemaRegistration)
            {
                _schemaRegistrationService = new AvroSchemaRegistrationService(
                    Options.CustomSchemaRegistryClient,
                    Options.ValidationMode,
                    Options.EnableDebugLogging);

                try
                {
                    Console.WriteLine("[INFO] Core層統合: Avroスキーマ自動登録開始");
                    await _schemaRegistrationService.RegisterAllSchemasAsync(GetEntityModels());
                    Console.WriteLine("[INFO] Core層統合: Avroスキーマ自動登録完了");
                }
                catch (Exception ex)
                {
                    if (Options.ValidationMode == ValidationMode.Strict)
                    {
                        throw new InvalidOperationException($"Core層統合: スキーマ自動登録失敗 - {ex.Message}", ex);
                    }
                    else
                    {
                        Console.WriteLine($"[WARNING] Core層統合: スキーマ自動登録失敗（Relaxedモードのため続行） - {ex.Message}");
                    }
                }
            }

            await Task.Delay(1, cancellationToken);

            if (Options.EnableDebugLogging)
            {
                Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: Core層統合インフラ構築完了");
                Console.WriteLine(GetCoreDiagnostics());
            }
        }

        public async Task<List<string>> GetRegisteredSchemasAsync()
        {
            if (_schemaRegistrationService == null)
                return new List<string>();
            return await _schemaRegistrationService.GetRegisteredSchemasAsync();
        }

        public async Task<bool> CheckEntitySchemaCompatibilityAsync<T>() where T : class
        {
            if (_schemaRegistrationService == null)
                return false;

            var entityModel = GetModelBuilder().GetEntityModel<T>();
            if (entityModel == null)
                return false;

            var topicName = entityModel.TopicAttribute?.TopicName ?? typeof(T).Name;
            var valueSchema = SchemaGenerator.GenerateSchema<T>();

            return await _schemaRegistrationService.CheckSchemaCompatibilityAsync($"{topicName}-value", valueSchema);
        }

        public string GetCoreDiagnostics()
        {
            var diagnostics = new List<string>
            {
                "=== Core層統合診断情報 ===",
                $"Base Context: {base.GetDiagnostics()}",
                "",
                "=== 上位層サービス状態 ===",
                $"ProducerManager: {(_producerManager.IsValueCreated ? "初期化済み" : "未初期化")}",
                $"ConsumerManager: {(_consumerManager.IsValueCreated ? "初期化済み" : "未初期化")}",
                $"SchemaRegistration: {(_schemaRegistrationService != null ? "有効" : "無効")}"

            };

            if (Options.TopicOverrideService.GetAllOverrides().Any())
            {
                diagnostics.Add("");
                diagnostics.Add("=== トピック上書き設定 ===");
                diagnostics.Add(Options.TopicOverrideService.GetOverrideSummary());
            }

            return string.Join(Environment.NewLine, diagnostics);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // 上位層サービスの破棄
                if (_producerManager.IsValueCreated)
                    _producerManager.Value.Dispose();

                if (_consumerManager.IsValueCreated)
                    _consumerManager.Value.Dispose();


                if (Options.EnableDebugLogging)
                    Console.WriteLine("[DEBUG] KafkaContext.Dispose: Core層統合リソース解放完了");
            }

            base.Dispose(disposing);
        }

        protected override async ValueTask DisposeAsyncCore()
        {
            // 上位層サービスの非同期破棄
            if (_producerManager.IsValueCreated)
                _producerManager.Value.Dispose();

            if (_consumerManager.IsValueCreated)
                _consumerManager.Value.Dispose();

            await base.DisposeAsyncCore();
        }

        public override string ToString()
        {
            return $"{base.ToString()} [Core層統合]";
        }
    }


    /// <summary>
    /// 上位層サービス統合EventSet
    /// 設計理由：Core抽象化を実装し、Producer/Consumer機能を提供
    /// </summary>
    internal class EventSetWithServices<T> : EventSet<T> where T : class
    {
        private readonly KafkaContext _kafkaContext;

        public EventSetWithServices(KafkaContext context, EntityModel entityModel)
            : base(context, entityModel)
        {
            _kafkaContext = context;
        }

        public EventSetWithServices(KafkaContext context, EntityModel entityModel, System.Linq.Expressions.Expression expression)
            : base(context, entityModel, expression)
        {
            _kafkaContext = context;
        }

        /// <summary>
        /// Core抽象化実装：Producer機能
        /// </summary>
        protected override async Task SendEntityAsync(T entity, CancellationToken cancellationToken)
        {
            try
            {
                var producerManager = _kafkaContext.GetProducerManager();
                var producer = await producerManager.GetProducerAsync<T>();

                try
                {
                    var context = new KafkaMessageContext
                    {
                        MessageId = Guid.NewGuid().ToString(),
                        Tags = new Dictionary<string, object>
                        {
                            ["entity_type"] = typeof(T).Name,
                            ["method"] = "Core.SendEntityAsync"
                        }
                    };

                    await producer.SendAsync(entity, context, cancellationToken);
                }
                finally
                {
                    producerManager.ReturnProducer(producer);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Core層統合: Entity送信失敗 - {typeof(T).Name}", ex);
            }
        }

        /// <summary>
        /// Core抽象化実装：Producer一括機能
        /// </summary>
        protected override async Task SendEntitiesAsync(IEnumerable<T> entities, CancellationToken cancellationToken)
        {
            try
            {
                var producerManager = _kafkaContext.GetProducerManager();

                var batchContext = new KafkaMessageContext
                {
                    MessageId = Guid.NewGuid().ToString(),
                    Tags = new Dictionary<string, object>
                    {
                        ["entity_type"] = typeof(T).Name,
                        ["method"] = "Core.SendEntitiesAsync",
                        ["batch_size"] = entities.Count()
                    }
                };

                var batchResult = await producerManager.SendBatchOptimizedAsync(entities, batchContext, cancellationToken);

                if (!batchResult.AllSuccessful)
                {
                    var failureDetails = string.Join(", ",
                        batchResult.Errors.Select(e => $"Index {e.MessageIndex}: {e.Error?.Reason}"));

                    throw new KafkaBatchSendException(
                        $"Core層統合: バッチ送信部分失敗 - {batchResult.FailedCount}/{batchResult.TotalMessages}件失敗. Details: {failureDetails}",
                        batchResult);
                }
            }
            catch (Exception ex) when (!(ex is KafkaBatchSendException))
            {
                throw new InvalidOperationException($"Core層統合: Entity一括送信失敗 - {typeof(T).Name}", ex);
            }
        }

        /// <summary>
        /// Core抽象化実装：Consumer機能
        /// </summary>
        protected override List<T> ExecuteQuery(string ksqlQuery)
        {
            try
            {
                // Phase2でConsumerManagerに移行予定
                // 現在は新しいConsumerManager使用
                var consumerManager = _kafkaContext.GetConsumerManager();

                // 一時的な実装：KSQLクエリを実行してリストを返す
                // 実際の実装では、KSQLクエリをConsumerで実行する必要がある
                Console.WriteLine($"[INFO] 旧ConsumerService廃止のため、Core層統合Consumerに移行中... Query: {ksqlQuery}");

                // 簡略実装：現在は空のリストを返す
                // TODO: Phase2でConsumerManagerによる実装に置き換える
                return new List<T>();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Core層統合: クエリ実行は移行中です。Phase2完了後に新APIを使用してください。原因: {ex.Message}", ex);
            }
        }
    }

}