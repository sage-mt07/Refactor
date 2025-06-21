// src/KafkaContext.cs - Core層統合完全版
using KsqlDsl.Configuration;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Context;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Messaging.Consumers;
using System;
using System.Collections.Generic;
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
        private readonly KafkaProducerManager _producerManager;
        private readonly KafkaConsumerManager _consumerManager;

        private readonly ModelBuilder _modelBuilder;
        protected KafkaContext() : base()
        {
            _modelBuilder = new ModelBuilder(ValidationMode.Strict); // デフォルト値
            _producerManager = new KafkaProducerManager(
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                null);

            _consumerManager = new KafkaConsumerManager(
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                null);
        }

        protected KafkaContext(KafkaContextOptions options) : base(options)
        {
            _producerManager = new KafkaProducerManager(
              Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
              null);

            _consumerManager = new KafkaConsumerManager(
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                null);
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

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _producerManager.Dispose();
                _consumerManager.Dispose();


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
            : base(context, entityModel)
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
    }

}