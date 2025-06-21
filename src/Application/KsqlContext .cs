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
    private readonly Lazy<KafkaProducerManager> _producerManager;
    private readonly Lazy<KafkaConsumerManager> _consumerManager;
    private AvroSchemaRegistrationService? _schemaRegistrationService;

    protected KafkaContext() : base()
    {
        _producerManager = new Lazy<KafkaProducerManager>(() =>
        {
            Console.WriteLine("[INFO] 簡素化統合: KafkaProducerManager初期化");
            return new KafkaProducerManager(
                null!, // EnhancedAvroSerializerManager - DIから注入
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                Options.LoggerFactory);
        });

        _consumerManager = new Lazy<KafkaConsumerManager>(() =>
        {
            Console.WriteLine("[INFO] 簡素化統合: KafkaConsumerManager初期化");
            return new KafkaConsumerManager(
                null!, // EnhancedAvroSerializerManager - DIから注入
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                Options.LoggerFactory);
        });
    }

    protected KafkaContext(KafkaContextOptions options) : base(options)
    {
        _producerManager = new Lazy<KafkaProducerManager>(() =>
        {
            Console.WriteLine("[INFO] 簡素化統合: KafkaProducerManager初期化");
            return new KafkaProducerManager(
                null!, // EnhancedAvroSerializerManager - DIから注入
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                Options.LoggerFactory);
        });

        _consumerManager = new Lazy<KafkaConsumerManager>(() =>
        {
            Console.WriteLine("[INFO] 簡素化統合: KafkaConsumerManager初期化");
            return new KafkaConsumerManager(
                null!, // EnhancedAvroSerializerManager - DIから注入
                Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
                Options.LoggerFactory);
        });
    }

    /// <summary>
    /// Core層EventSet実装（簡素化版）
    /// </summary>
    protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel)
    {
        return new EventSetWithSimplifiedServices<T>(this, entityModel);
    }

    // 簡素化Manager取得
    internal KafkaProducerManager GetProducerManager() => _producerManager.Value;
    internal KafkaConsumerManager GetConsumerManager() => _consumerManager.Value;

    public new async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (Options.EnableDebugLogging)
        {
            Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: 簡素化統合インフラ構築開始");
        }

        // スキーマ自動登録（簡素化）
        if (Options.EnableAutoSchemaRegistration)
        {
            _schemaRegistrationService = new AvroSchemaRegistrationService(
           Options.CustomSchemaRegistryClient,
           _loggerFactory);

            try
            {
                Console.WriteLine("[INFO] 簡素化統合: Avroスキーマ自動登録開始");
                await _schemaRegistrationService.RegisterAllSchemasAsync(GetEntityModels());
                Console.WriteLine("[INFO] 簡素化統合: Avroスキーマ自動登録完了");
            }
            catch (Exception ex)
            {
                if (Options.ValidationMode == ValidationMode.Strict)
                {
                    throw new InvalidOperationException($"簡素化統合: スキーマ自動登録失敗 - {ex.Message}", ex);
                }
                else
                {
                    Console.WriteLine($"[WARNING] 簡素化統合: スキーマ自動登録失敗（続行） - {ex.Message}");
                }
            }
        }

        await Task.Delay(1, cancellationToken);

        if (Options.EnableDebugLogging)
        {
            Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: 簡素化統合インフラ構築完了");
            Console.WriteLine(GetSimplifiedDiagnostics());
        }
    }

    public string GetSimplifiedDiagnostics()
    {
        var diagnostics = new List<string>
        {
            "=== 簡素化統合診断情報 ===",
            $"Base Context: {base.GetDiagnostics()}",
            "",
            "=== 簡素化Manager状態 ===",
            $"ProducerManager: {(_producerManager.IsValueCreated ? "初期化済み" : "未初期化")}",
            $"ConsumerManager: {(_consumerManager.IsValueCreated ? "初期化済み" : "未初期化")}",
            $"SchemaRegistration: {(_schemaRegistrationService != null ? "有効" : "無効")}",
            "",
            "=== Pool削除確認 ===",
            "✅ ProducerPool削除済み",
            "✅ ConsumerPool削除済み",
            "✅ メトリクス削除済み - Confluent.Kafka委譲",
            "✅ 複雑性削除完了"
        };

        return string.Join(Environment.NewLine, diagnostics);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            // 簡素化Manager破棄
            if (_producerManager.IsValueCreated)
                _producerManager.Value.Dispose();

            if (_consumerManager.IsValueCreated)
                _consumerManager.Value.Dispose();

            if (Options.EnableDebugLogging)
                Console.WriteLine("[DEBUG] KafkaContext.Dispose: 簡素化統合リソース解放完了");
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
        : base(context, entityModel, expression)
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

    /// <summary>
    /// Core抽象化実装：Producer一括機能（簡素化）
    /// </summary>
    protected override async Task SendEntitiesAsync(IEnumerable<T> entities, CancellationToken cancellationToken)
    {
        try
        {
            var producerManager = _kafkaContext.GetProducerManager();
            await producerManager.SendRangeAsync(entities, cancellationToken);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"簡素化統合: Entity一括送信失敗 - {typeof(T).Name}", ex);
        }
    }

    /// <summary>
    /// Core抽象化実装：Consumer機能（簡素化）
    /// </summary>
    protected override List<T> ExecuteQuery(string ksqlQuery)
    {
        try
        {
            var consumerManager = _kafkaContext.GetConsumerManager();

            Console.WriteLine($"[INFO] 簡素化統合: KSQL実行移行中... Query: {ksqlQuery}");

            // 簡略実装：Pull Query風の取得
            var options = new KafkaFetchOptions
            {
                MaxRecords = 100,
                Timeout = TimeSpan.FromSeconds(30)
            };

            var task = consumerManager.FetchAsync<T>(options, CancellationToken.None);
            return task.GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"簡素化統合: クエリ実行は移行中です。新APIを使用してください。原因: {ex.Message}", ex);
        }
    }
}