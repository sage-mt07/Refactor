using Confluent.Kafka;
using KsqlDsl.Configuration;
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Configuration;
using KsqlDsl.Messaging.Consumers.Core;
using KsqlDsl.Messaging.Producers.Core;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers
{
    /// <summary>
    /// 簡素化Consumer管理 - Pool削除、直接管理
    /// 設計理由: EF風API、事前確定管理、複雑性削除
    /// </summary>
    public class KafkaConsumerManager : IDisposable
    {
        private readonly IAvroSerializationManager<object> _serializerManager;
        private readonly KsqlDslOptions _options;
        private readonly ILogger? _logger;
        private readonly ConcurrentDictionary<Type, object> _consumers = new();
        private bool _disposed = false;

        public KafkaConsumerManager(
            IAvroSerializationManager<object> serializerManager,
            IOptions<KsqlDslOptions> options,
            ILoggerFactory? loggerFactory = null)
        {
            _serializerManager = serializerManager ?? throw new ArgumentNullException(nameof(serializerManager));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = loggerFactory.CreateLoggerOrNull<KafkaConsumerManager>();

            _logger?.LogInformation("Simplified KafkaConsumerManager initialized");
        }

        /// <summary>
        /// 型安全Consumer取得 - 事前確定・キャッシュ
        /// </summary>
        public async Task<IKafkaConsumer<T>> GetConsumerAsync<T>(KafkaSubscriptionOptions? options = null) where T : class
        {
            var entityType = typeof(T);
            var cacheKey = $"{entityType.Name}_{options?.GroupId ?? "default"}";

            if (_consumers.TryGetValue(entityType, out var cachedConsumer))
            {
                return (IKafkaConsumer<T>)cachedConsumer;
            }

            try
            {
                var entityModel = GetEntityModel<T>();
                var topicName = entityModel.TopicAttribute?.TopicName ?? entityType.Name;

                // Confluent.Kafka Consumer作成
                var config = BuildConsumerConfig(topicName, options);
                var rawConsumer = new ConsumerBuilder<object, object>(config).Build();

                // Avroデシリアライザー取得
                var (keyDeserializer, valueDeserializer) = await _serializerManager.CreateDeserializersAsync<T>(entityModel);

                // 統合Consumer作成
                var consumer = new KafkaConsumer<T>(
                    rawConsumer,
                    keyDeserializer,
                    valueDeserializer,
                    topicName,
                    entityModel,
                    _logger?.Factory);

                _consumers.TryAdd(entityType, consumer);

                _logger?.LogDebug("Consumer created: {EntityType} -> {TopicName}", entityType.Name, topicName);
                return consumer;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to create consumer: {EntityType}", entityType.Name);
                throw;
            }
        }

        /// <summary>
        /// エンティティ取得 - EventSetから使用
        /// </summary>
        public async IAsyncEnumerable<T> ConsumeAsync<T>([EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
        {
            var consumer = await GetConsumerAsync<T>();

            await foreach (var kafkaMessage in consumer.ConsumeAsync(cancellationToken))
            {
                yield return kafkaMessage.Value;
            }
        }

        /// <summary>
        /// エンティティ一覧取得 - EventSetから使用
        /// </summary>
        public async Task<List<T>> FetchAsync<T>(KafkaFetchOptions options, CancellationToken cancellationToken = default) where T : class
        {
            var consumer = await GetConsumerAsync<T>();
            var batchOptions = new KafkaBatchOptions
            {
                MaxBatchSize = options.MaxRecords,
                MaxWaitTime = options.Timeout,
                EnableEmptyBatches = false
            };

            var batch = await consumer.ConsumeBatchAsync(batchOptions, cancellationToken);
            var results = new List<T>();

            foreach (var message in batch.Messages)
            {
                results.Add(message.Value);
            }

            return results;
        }

        /// <summary>
        /// 購読開始
        /// </summary>
        public async Task SubscribeAsync<T>(
            Func<T, KafkaMessageContext, Task> handler,
            KafkaSubscriptionOptions? options = null,
            CancellationToken cancellationToken = default) where T : class
        {
            if (handler == null)
                throw new ArgumentNullException(nameof(handler));

            var consumer = await GetConsumerAsync<T>(options);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var kafkaMessage in consumer.ConsumeAsync(cancellationToken))
                    {
                        try
                        {
                            await handler(kafkaMessage.Value, kafkaMessage.Context ?? new KafkaMessageContext());
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, "Message handler failed: {EntityType}", typeof(T).Name);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger?.LogInformation("Subscription cancelled: {EntityType}", typeof(T).Name);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Subscription error: {EntityType}", typeof(T).Name);
                }
            }, cancellationToken);
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

        private ConsumerConfig BuildConsumerConfig(string topicName, KafkaSubscriptionOptions? subscriptionOptions)
        {
            var topicConfig = _options.Topics.TryGetValue(topicName, out var config)
                ? config
                : new TopicSection();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _options.Common.BootstrapServers,
                ClientId = _options.Common.ClientId,
                GroupId = subscriptionOptions?.GroupId ?? topicConfig.Consumer.GroupId ?? "default-group",
                AutoOffsetReset = Enum.Parse<AutoOffsetReset>(topicConfig.Consumer.AutoOffsetReset),
                EnableAutoCommit = topicConfig.Consumer.EnableAutoCommit,
                AutoCommitIntervalMs = topicConfig.Consumer.AutoCommitIntervalMs,
                SessionTimeoutMs = topicConfig.Consumer.SessionTimeoutMs,
                HeartbeatIntervalMs = topicConfig.Consumer.HeartbeatIntervalMs,
                MaxPollIntervalMs = topicConfig.Consumer.MaxPollIntervalMs,
                FetchMinBytes = topicConfig.Consumer.FetchMinBytes,
                FetchMaxWaitMs = topicConfig.Consumer.FetchMaxWaitMs,
                FetchMaxBytes = topicConfig.Consumer.FetchMaxBytes,
                IsolationLevel = Enum.Parse<IsolationLevel>(topicConfig.Consumer.IsolationLevel)
            };

            // 購読オプション適用
            if (subscriptionOptions != null)
            {
                if (subscriptionOptions.AutoCommit.HasValue)
                    consumerConfig.EnableAutoCommit = subscriptionOptions.AutoCommit.Value;
                if (subscriptionOptions.SessionTimeout.HasValue)
                    consumerConfig.SessionTimeoutMs = (int)subscriptionOptions.SessionTimeout.Value.TotalMilliseconds;
                if (subscriptionOptions.HeartbeatInterval.HasValue)
                    consumerConfig.HeartbeatIntervalMs = (int)subscriptionOptions.HeartbeatInterval.Value.TotalMilliseconds;
                if (subscriptionOptions.MaxPollInterval.HasValue)
                    consumerConfig.MaxPollIntervalMs = (int)subscriptionOptions.MaxPollInterval.Value.TotalMilliseconds;
            }

            // 追加設定適用
            foreach (var kvp in topicConfig.Consumer.AdditionalProperties)
            {
                consumerConfig.Set(kvp.Key, kvp.Value);
            }

            // セキュリティ設定
            if (_options.Common.SecurityProtocol != SecurityProtocol.Plaintext)
            {
                consumerConfig.SecurityProtocol = _options.Common.SecurityProtocol;
                if (_options.Common.SaslMechanism.HasValue)
                {
                    consumerConfig.SaslMechanism = _options.Common.SaslMechanism.Value;
                    consumerConfig.SaslUsername = _options.Common.SaslUsername;
                    consumerConfig.SaslPassword = _options.Common.SaslPassword;
                }

                if (!string.IsNullOrEmpty(_options.Common.SslCaLocation))
                {
                    consumerConfig.SslCaLocation = _options.Common.SslCaLocation;
                    consumerConfig.SslCertificateLocation = _options.Common.SslCertificateLocation;
                    consumerConfig.SslKeyLocation = _options.Common.SslKeyLocation;
                    consumerConfig.SslKeyPassword = _options.Common.SslKeyPassword;
                }
            }

            return consumerConfig;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _logger?.LogInformation("Disposing simplified KafkaConsumerManager...");

                foreach (var consumer in _consumers.Values)
                {
                    if (consumer is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }
                _consumers.Clear();

                _disposed = true;
                _logger?.LogInformation("KafkaConsumerManager disposed");
            }
        }
    }
}