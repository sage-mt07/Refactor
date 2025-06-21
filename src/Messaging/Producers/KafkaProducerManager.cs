using Confluent.Kafka;
using KsqlDsl.Configuration;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Messaging.Configuration;
using KsqlDsl.Messaging.Producers.Core;
using KsqlDsl.Serialization.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;
/// <summary>
/// 型安全Producer管理 - Pool削除、直接管理、型安全性強化版
/// 設計理由: EF風API、事前確定管理、型安全性確保
/// </summary>
public class KafkaProducerManager : IDisposable
{
    private readonly KsqlDslOptions _options;
    private readonly ILogger? _logger;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly ConcurrentDictionary<Type, object> _producers = new();
    private readonly ConcurrentDictionary<Type, object> _serializationManagers = new();
    private readonly Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient> _schemaRegistryClient;
    private bool _disposed = false;

    // ✅ 修正：コンストラクタからIAvroSerializationManager<object>を削除
    public KafkaProducerManager(
        IOptions<KsqlDslOptions> options,
        ILoggerFactory? loggerFactory = null)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = loggerFactory.CreateLoggerOrNull<KafkaProducerManager>();
        _loggerFactory = loggerFactory;

        // SchemaRegistryClientの遅延初期化
        _schemaRegistryClient = new Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient>(CreateSchemaRegistryClient);

        _logger?.LogInformation("Type-safe KafkaProducerManager initialized");
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

            // ✅ 追加：型安全なシリアライゼーションマネージャー取得
            var serializationManager = GetOrCreateSerializationManager<T>();
            var serializerPair = await serializationManager.GetSerializersAsync();

            // 統合Producer作成
            var producer = new KafkaProducer<T>(
                rawProducer,
                serializerPair.KeySerializer,
                serializerPair.ValueSerializer,
                topicName,
                entityModel,
                _loggerFactory);

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
    /// Producer設定構築
    /// </summary>
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
            RetryBackoffMs = topicConfig.Producer.RetryBackoffMs
        };

        // パーティショナー設定
        if (!string.IsNullOrEmpty(topicConfig.Producer.Partitioner))
        {
            // パーティショナーは文字列またはクラス名として設定
            producerConfig.Set("partitioner.class", topicConfig.Producer.Partitioner);
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

        // 追加設定適用
        foreach (var kvp in topicConfig.Producer.AdditionalProperties)
        {
            producerConfig.Set(kvp.Key, kvp.Value);
        }

        return producerConfig;
    }
    // ✅ 追加：型安全なシリアライゼーションマネージャー取得（ConsumerManagerと同様）
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

    // ✅ 追加：SchemaRegistryClient作成（ConsumerManagerと同様）
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

            if (!string.IsNullOrEmpty(_options.SchemaRegistry.SslKeystoreLocation))
            {
                config.SslKeystoreLocation = _options.SchemaRegistry.SslKeystoreLocation;
            }

            if (!string.IsNullOrEmpty(_options.SchemaRegistry.SslKeystorePassword))
            {
                config.SslKeystorePassword = _options.SchemaRegistry.SslKeystorePassword;
            }
        }

        // 追加プロパティ
        foreach (var kvp in _options.SchemaRegistry.AdditionalProperties)
        {
            config.Set(kvp.Key, kvp.Value);
        }

        // SslKeyPasswordをAdditionalPropertyとして設定
        if (!string.IsNullOrEmpty(_options.SchemaRegistry.SslKeyPassword))
        {
            config.Set("ssl.key.password", _options.SchemaRegistry.SslKeyPassword);
        }

        _logger?.LogDebug("Created SchemaRegistryClient with URL: {Url}", config.Url);
        return new ConfluentSchemaRegistry.CachedSchemaRegistryClient(config);
    }

    // ✅ 追加：EntityModel作成（ConsumerManagerと同様）
    private EntityModel GetEntityModel<T>() where T : class
    {
        var entityType = typeof(T);
        var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
        var allProperties = entityType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

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
    // 既存のメソッドは変更なし（SendAsync, SendRangeAsync, BuildProducerConfig）

    // ✅ 修正：Disposeメソッドの更新
    public void Dispose()
    {
        if (!_disposed)
        {
            _logger?.LogInformation("Disposing type-safe KafkaProducerManager...");

            // Producerの解放
            foreach (var producer in _producers.Values)
            {
                if (producer is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _producers.Clear();

            // ✅ 追加：SerializationManagerの解放
            foreach (var manager in _serializationManagers.Values)
            {
                if (manager is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _serializationManagers.Clear();

            // ✅ 追加：SchemaRegistryClientの解放
            if (_schemaRegistryClient.IsValueCreated)
            {
                _schemaRegistryClient.Value?.Dispose();
            }

            _disposed = true;
            _logger?.LogInformation("Type-safe KafkaProducerManager disposed");
        }
    }
}