using Confluent.Kafka;
using KsqlDsl.Configuration;
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Configuration;
using KsqlDsl.Messaging.Consumers.Core;
using KsqlDsl.Serialization.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Messaging.Consumers
{
    /// <summary>
    /// 型安全Consumer管理 - Pool削除、直接管理、型安全性強化版
    /// 設計理由: EF風API、事前確定管理、型安全性確保
    /// </summary>
    public class KafkaConsumerManager : IDisposable
    {
        private readonly KsqlDslOptions _options;
        private readonly ILogger? _logger;
        private readonly ILoggerFactory? _loggerFactory;
        private readonly ConcurrentDictionary<Type, object> _consumers = new();
        private readonly ConcurrentDictionary<Type, object> _serializationManagers = new();
        private readonly Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient> _schemaRegistryClient;
        private bool _disposed = false;

        public KafkaConsumerManager(
            IOptions<KsqlDslOptions> options,
            ILoggerFactory? loggerFactory = null)
        {
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = loggerFactory.CreateLoggerOrNull<KafkaConsumerManager>();
            _loggerFactory = loggerFactory;

            // SchemaRegistryClientの遅延初期化
            _schemaRegistryClient = new Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient>(CreateSchemaRegistryClient);

            _logger?.LogInformation("Type-safe KafkaConsumerManager initialized");
        }

        /// <summary>
        /// 型安全Consumer取得 - 事前確定・キャッシュ
        /// </summary>
        public async Task<IKafkaConsumer<T, object>> GetConsumerAsync<T>(KafkaSubscriptionOptions? options = null) where T : class
        {
            var entityType = typeof(T);

            if (_consumers.TryGetValue(entityType, out var cachedConsumer))
            {
                return (IKafkaConsumer<T, object>)cachedConsumer;
            }

            try
            {
                var entityModel = GetEntityModel<T>();
                var topicName = entityModel.TopicAttribute?.TopicName ?? entityType.Name;

                // Confluent.Kafka Consumer作成
                var config = BuildConsumerConfig(topicName, options);
                var rawConsumer = new ConsumerBuilder<object, object>(config).Build();

                // 型安全なシリアライゼーションマネージャー取得
                var serializationManager = GetOrCreateSerializationManager<T>();
                var deserializerPair = await serializationManager.GetDeserializersAsync();

                // 統合Consumer作成
                var consumer = new KafkaConsumer<T, object>(
                    rawConsumer,
                    deserializerPair.KeyDeserializer,
                    deserializerPair.ValueDeserializer,
                    topicName,
                    entityModel,
                    _loggerFactory);

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

        /// <summary>
        /// 型安全なシリアライゼーションマネージャー取得
        /// </summary>
        private IAvroSerializationManager<T> GetOrCreateSerializationManager<T>() where T : class
        {
            var entityType = typeof(T);

            if (_serializationManagers.TryGetValue(entityType, out var existingManager))
            {
                return (IAvroSerializationManager<T>)existingManager;
            }

            var newManager = new AvroSerializationManager<T>(_schemaRegistryClient.Value, _loggerFactory);
            _serializationManagers.TryAdd(entityType, newManager);

            _logger?.LogDebug("Created SerializationManager for {EntityType}", entityType.Name);
            return newManager;
        }

        /// <summary>
        /// SchemaRegistryClient作成
        /// </summary>
        private ConfluentSchemaRegistry.ISchemaRegistryClient CreateSchemaRegistryClient()
        {
            var config = new ConfluentSchemaRegistry.SchemaRegistryConfig
            {
                Url = _options.SchemaRegistry.Url,
                MaxCachedSchemas = _options.SchemaRegistry.MaxCachedSchemas,
                RequestTimeoutMs = _options.SchemaRegistry.RequestTimeoutMs
            };

            // Basic認証設定
            if (!string.IsNullOrEmpty(_options.SchemaRegistry.BasicAuthUserInfo))
            {
                config.BasicAuthUserInfo = _options.SchemaRegistry.BasicAuthUserInfo;
                config.BasicAuthCredentialsSource = (ConfluentSchemaRegistry.AuthCredentialsSource)_options.SchemaRegistry.BasicAuthCredentialsSource;
            }

            // SSL設定
            if (!string.IsNullOrEmpty(_options.SchemaRegistry.SslCaLocation))
            {
                config.SslCaLocation = _options.SchemaRegistry.SslCaLocation;
                config.SslKeystoreLocation = _options.SchemaRegistry.SslKeystoreLocation;
                config.SslKeystorePassword = _options.SchemaRegistry.SslKeystorePassword;
            }

            // 追加プロパティ
            foreach (var kvp in _options.SchemaRegistry.AdditionalProperties)
            {
                config.Set(kvp.Key, kvp.Value);
            }

            _logger?.LogDebug("Created SchemaRegistryClient with URL: {Url}", config.Url);
            return new ConfluentSchemaRegistry.CachedSchemaRegistryClient(config);
        }

        /// <summary>
        /// EntityModel作成（簡略実装）
        /// </summary>
        private EntityModel GetEntityModel<T>() where T : class
        {
            var entityType = typeof(T);
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            var allProperties = entityType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

            // Key プロパティをOrder順にソート
            Array.Sort(keyProperties, (p1, p2) =>
            {
                var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                return order1.CompareTo(order2);
            });

            return new EntityModel
            {
                EntityType = entityType,
                TopicAttribute = topicAttribute,
                KeyProperties = keyProperties,
                AllProperties = allProperties
            };
        }

        /// <summary>
        /// Consumer設定構築
        /// </summary>
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

            // 追加設定適用
            foreach (var kvp in topicConfig.Consumer.AdditionalProperties)
            {
                consumerConfig.Set(kvp.Key, kvp.Value);
            }

            return consumerConfig;
        }

        /// <summary>
        /// リソース解放
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _logger?.LogInformation("Disposing type-safe KafkaConsumerManager...");

                // Consumerの解放
                foreach (var consumer in _consumers.Values)
                {
                    if (consumer is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }
                _consumers.Clear();

                // SerializationManagerの解放
                foreach (var manager in _serializationManagers.Values)
                {
                    if (manager is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }
                _serializationManagers.Clear();

                // SchemaRegistryClientの解放
                if (_schemaRegistryClient.IsValueCreated)
                {
                    _schemaRegistryClient.Value?.Dispose();
                }

                _disposed = true;
                _logger?.LogInformation("Type-safe KafkaConsumerManager disposed");
            }
        }
    }
}