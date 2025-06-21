using Confluent.Kafka;
using KsqlDsl.Configuration;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Core.Models;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Configuration;
using KsqlDsl.Messaging.Producers.Core;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Producers
{
    /// <summary>
    /// 簡素化Producer管理 - Pool削除、直接管理
    /// 設計理由: EF風API、事前確定管理、複雑性削除
    /// </summary>
    public class KafkaProducerManager : IDisposable
    {
        private readonly IAvroSerializationManager<object> _serializerManager;
        private readonly KsqlDslOptions _options;
        private readonly ILogger? _logger;
        private readonly ConcurrentDictionary<Type, object> _producers = new();
        private bool _disposed = false;

        public KafkaProducerManager(
            IAvroSerializationManager<object> serializerManager,
            IOptions<KsqlDslOptions> options,
            ILoggerFactory? loggerFactory = null)
        {
            _serializerManager = serializerManager ?? throw new ArgumentNullException(nameof(serializerManager));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = loggerFactory.CreateLoggerOrNull<KafkaProducerManager>();

            _logger?.LogInformation("Simplified KafkaProducerManager initialized");
        }

        /// <summary>
        /// 型安全Producer取得 - 事前確定・キャッシュ
        /// </summary>
        public async Task<IKafkaProducer<T>> GetProducerAsync<T>() where T : class
        {
            var entityType = typeof(T);

            if (_producers.TryGetValue(entityType, out var cachedProducer))
            {
                return (IKafkaProducer<T>)cachedProducer;
            }

            try
            {
                var entityModel = GetEntityModel<T>();
                var topicName = entityModel.TopicAttribute?.TopicName ?? entityType.Name;

                // Confluent.Kafka Producer作成
                var config = BuildProducerConfig(topicName);
                var rawProducer = new ProducerBuilder<object, object>(config).Build();

                // Avroシリアライザー取得
                var (keySerializer, valueSerializer) = await _serializerManager.CreateSerializersAsync<T>(entityModel);

                // 統合Producer作成
                var producer = new KafkaProducer<T>(
                    rawProducer,
                    keySerializer,
                    valueSerializer,
                    topicName,
                    entityModel,
                    _logger?.Factory);

                _producers.TryAdd(entityType, producer);

                _logger?.LogDebug("Producer created: {EntityType} -> {TopicName}", entityType.Name, topicName);
                return producer;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to create producer: {EntityType}", entityType.Name);
                throw;
            }
        }

        /// <summary>
        /// エンティティ送信 - EventSetから使用
        /// </summary>
        public async Task SendAsync<T>(T entity, CancellationToken cancellationToken = default) where T : class
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            var producer = await GetProducerAsync<T>();
            var context = new KafkaMessageContext
            {
                MessageId = Guid.NewGuid().ToString(),
                Tags = new Dictionary<string, object>
                {
                    ["entity_type"] = typeof(T).Name,
                    ["method"] = "SendAsync"
                }
            };

            await producer.SendAsync(entity, context, cancellationToken);
        }

        /// <summary>
        /// エンティティ一括送信 - EventSetから使用
        /// </summary>
        public async Task SendRangeAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            var producer = await GetProducerAsync<T>();
            var context = new KafkaMessageContext
            {
                MessageId = Guid.NewGuid().ToString(),
                Tags = new Dictionary<string, object>
                {
                    ["entity_type"] = typeof(T).Name,
                    ["method"] = "SendRangeAsync"
                }
            };

            await producer.SendBatchAsync(entities, context, cancellationToken);
        }

        private EntityModel GetEntityModel<T>() where T : class
        {
            // 簡略実装 - 実際の実装ではModelBuilderから取得
            return new EntityModel
            {
                EntityType = typeof(T),
                TopicAttribute = new TopicAttribute(typeof(T).Name)
            };
        }

        private ProducerConfig BuildProducerConfig(string topicName)
        {
            var topicConfig = _options.Topics.TryGetValue(topicName, out var config)
                ? config
                : new TopicSection();

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _options.Common.BootstrapServers,
                ClientId = _options.Common.ClientId,
                Acks = Enum.Parse<Acks>(topicConfig.Producer.Acks),
                CompressionType = Enum.Parse<CompressionType>(topicConfig.Producer.CompressionType),
                EnableIdempotence = topicConfig.Producer.EnableIdempotence,
                MaxInFlight = topicConfig.Producer.MaxInFlightRequestsPerConnection,
                LingerMs = topicConfig.Producer.LingerMs,
                BatchSize = topicConfig.Producer.BatchSize,
                DeliveryTimeoutMs = topicConfig.Producer.DeliveryTimeoutMs,
                RetryBackoffMs = topicConfig.Producer.RetryBackoffMs,
                Retries = topicConfig.Producer.Retries,
                BufferMemory = topicConfig.Producer.BufferMemory
            };

            // 追加設定適用
            foreach (var kvp in topicConfig.Producer.AdditionalProperties)
            {
                producerConfig.Set(kvp.Key, kvp.Value);
            }

            // セキュリティ設定
            if (_options.Common.SecurityProtocol != SecurityProtocol.Plaintext)
            {
                producerConfig.SecurityProtocol = _options.Common.SecurityProtocol;
                if (_options.Common.SaslMechanism.HasValue)
                {
                    producerConfig.SaslMechanism = _options.Common.SaslMechanism.Value;
                    producerConfig.SaslUsername = _options.Common.SaslUsername;
                    producerConfig.SaslPassword = _options.Common.SaslPassword;
                }

                if (!string.IsNullOrEmpty(_options.Common.SslCaLocation))
                {
                    producerConfig.SslCaLocation = _options.Common.SslCaLocation;
                    producerConfig.SslCertificateLocation = _options.Common.SslCertificateLocation;
                    producerConfig.SslKeyLocation = _options.Common.SslKeyLocation;
                    producerConfig.SslKeyPassword = _options.Common.SslKeyPassword;
                }
            }

            return producerConfig;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _logger?.LogInformation("Disposing simplified KafkaProducerManager...");

                foreach (var producer in _producers.Values)
                {
                    if (producer is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }
                _producers.Clear();

                _disposed = true;
                _logger?.LogInformation("KafkaProducerManager disposed");
            }
        }
    }
}