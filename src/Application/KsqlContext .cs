// src/Application/KsqlContext.cs - 簡素化・統合版
// 重複削除、未実装参照削除、Monitoring初期化除去済み

using KsqlDsl.Configuration;
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Context;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Messaging.Consumers;
using KsqlDsl.Messaging.Producers;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Cache;
using KsqlDsl.Serialization.Avro.Core;
using KsqlDsl.Serialization.Avro.Management;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Application;

// src/KafkaContext.cs への更新部分

/// <summary>
/// 簡素化統合KafkaContext
/// 設計理由: Pool削除、直接管理、EF風API
/// </summary>
public abstract class KafkaContext : KafkaContextCore
{
    // 簡素化Manager（Pool削除版）
    private readonly KafkaProducerManager _producerManager;
    private readonly KafkaConsumerManager _consumerManager;

    protected KafkaContext() : base()
    {
        _producerManager = new KafkaProducerManager(
            Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
            null   // LoggerFactory削除
        );

        _consumerManager = new KafkaConsumerManager(
            Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
            null   // LoggerFactory削除
        );
    }

    protected KafkaContext(KafkaContextOptions options) : base(options)
    {
        _producerManager = new KafkaProducerManager(
            Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
            null   // LoggerFactory削除
        );

        _consumerManager = new KafkaConsumerManager(
            Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
            null   // LoggerFactory削除
        );
    }

    /// <summary>
    /// Core層EventSet実装（簡素化版）
    /// </summary>
    protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel)
    {
        return new EventSetWithSimplifiedServices<T>(this, entityModel);
    }

    // 簡素化Manager取得
    internal KafkaProducerManager GetProducerManager() => _producerManager;
    internal KafkaConsumerManager GetConsumerManager() => _consumerManager;

 
    private IReadOnlyDictionary<Type, AvroEntityConfiguration> ConvertToAvroConfigurations(
    Dictionary<Type, EntityModel> entityModels)
    {
        var avroConfigs = new Dictionary<Type, AvroEntityConfiguration>();

        foreach (var kvp in entityModels)
        {
            var entityModel = kvp.Value;
            var avroConfig = new AvroEntityConfiguration(entityModel.EntityType)
            {
                TopicName = entityModel.TopicAttribute?.TopicName,
                KeyProperties = entityModel.KeyProperties
            };

            avroConfigs[kvp.Key] = avroConfig;
        }

        return avroConfigs;
    }


    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _producerManager?.Dispose();
            _consumerManager?.Dispose();
        }

        base.Dispose(disposing);
    }

    public override string ToString()
    {
        return $"{base.ToString()} [簡素化統合]";
    }
}

/// <summary>
/// 簡素化Manager統合EventSet
/// 設計理由: Pool削除、直接Manager使用、シンプル化
/// </summary>
internal class EventSetWithSimplifiedServices<T> : EventSet<T> where T : class
{
    private readonly KafkaContext _kafkaContext;

    public EventSetWithSimplifiedServices(KafkaContext context, EntityModel entityModel)
        : base(context, entityModel)
    {
        _kafkaContext = context;
    }

    public EventSetWithSimplifiedServices(KafkaContext context, EntityModel entityModel, System.Linq.Expressions.Expression expression)
        : base(context, entityModel)
    {
        _kafkaContext = context;
    }

    /// <summary>
    /// Core抽象化実装：Producer機能（簡素化）
    /// </summary>
    protected override async Task SendEntityAsync(T entity, CancellationToken cancellationToken)
    {
        try
        {
            var producerManager = _kafkaContext.GetProducerManager();
            await producerManager.SendAsync(entity, cancellationToken);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"簡素化統合: Entity送信失敗 - {typeof(T).Name}", ex);
        }
    }
   
}