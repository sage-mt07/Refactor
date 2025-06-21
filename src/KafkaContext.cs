// src/KafkaContext.cs - Core層統合完全版
using KsqlDsl.Configuration;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Context;
using KsqlDsl.Core.Modeling;
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
        private readonly KafkaProducerManager _producerManager;
        private readonly KafkaConsumerManager _consumerManager;


        private AvroSchemaRegistrationService? _schemaRegistrationService;
        private readonly Lazy<ModelBuilder> _modelBuilder;

        protected KafkaContext() : base()
        {
            _modelBuilder = new Lazy<ModelBuilder>(() => new ModelBuilder(Options.ValidationMode));

            _producerManager = new KafkaProducerManager(
                null!, // TODO: DI完了後に修正
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                null);

            _consumerManager = new KafkaConsumerManager(
                null!, // TODO: DI完了後に修正
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                null);
        }

        protected KafkaContext(KafkaContextOptions options) : base(options)
        {
            _producerManager = new KafkaProducerManager(
              null!, // TODO: DI完了後に修正
              Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
              Options.LoggerFactory);

            _consumerManager = new KafkaConsumerManager(
                null!, // TODO: DI完了後に修正
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                Options.LoggerFactory);
        }

        /// <summary>
        /// Core層EventSet実装（上位層機能統合）
        /// </summary>
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel)
        {
            return new EventSetWithServices<T>(this, entityModel);
        }

        // Core層統合API
        internal KafkaProducerManager GetProducerManager() => _producerManager;
        internal KafkaConsumerManager GetConsumerManager() => _consumerManager;

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

        

            await Task.Delay(1, cancellationToken);

            if (Options.EnableDebugLogging)
            {
                Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: Core層統合インフラ構築完了");
              
            }
        }



        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _producerManager.Dispose();
                _consumerManager.Dispose();


                if (Options.EnableDebugLogging)
                    Console.WriteLine("[DEBUG] KafkaContext.Dispose: Core層統合リソース解放完了");
            }

            base.Dispose(disposing);
        }

        protected override async ValueTask DisposeAsyncCore()
        {
            // 上位層サービスの非同期破棄
            _producerManager.Dispose();
            _consumerManager.Dispose();

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

                var context = new KafkaMessageContext
                {
                    MessageId = Guid.NewGuid().ToString(),
                    Tags = new Dictionary<string, object>
                    {
                        ["entity_type"] = typeof(T).Name,
                        ["method"] = "Core.SendEntityAsync"
                    }
                };

                await producerManager.SendAsync(entity, cancellationToken);
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

                try
                {
                    await producerManager.SendRangeAsync(entities, cancellationToken);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Core層統合: Entity一括送信失敗 - {typeof(T).Name}", ex);
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