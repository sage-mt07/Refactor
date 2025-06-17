// EventSet.cs の Producer関連メソッドのみの変更（Phase1用）
// AddAsync と AddRangeAsync メソッドを新ProducerManager使用に変更

using KsqlDsl.Modeling;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl;

public partial class EventSet<T> where T : class
{
    // 既存フィールドは変更なし
    private readonly KafkaContext _context;
    private readonly EntityModel _entityModel;
    // ... その他のフィールド

    /// <summary>
    /// Phase1変更：新ProducerManager使用
    /// 単一エンティティ追加（Producer送信）
    /// </summary>
    public async Task AddAsync(T entity, CancellationToken cancellationToken = default)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        ValidateEntity(entity);

        try
        {
            // Phase1変更：旧ProducerService → 新ProducerManager使用
            var producerManager = _context.GetProducerManager();
            var producer = await producerManager.GetProducerAsync<T>();

            try
            {
                var context = new KsqlDsl.Communication.KafkaMessageContext
                {
                    MessageId = Guid.NewGuid().ToString(),
                    Tags = new Dictionary<string, object>
                    {
                        ["entity_type"] = typeof(T).Name,
                        ["method"] = "AddAsync"
                    }
                };

                var result = await producer.SendAsync(entity, context, cancellationToken);

                if (_context.Options.EnableDebugLogging)
                {
                    var topicName = _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;
                    Console.WriteLine($"[DEBUG] EventSet.AddAsync: {typeof(T).Name} → Topic: {topicName} " +
                                    $"(Partition: {result.Partition}, Offset: {result.Offset}) - Phase1: 新ProducerManager使用");
                }
            }
            finally
            {
                // Phase1変更：新ProducerManagerでのProducer返却
                producerManager.ReturnProducer(producer);
            }
        }
        catch (Exception ex)
        {
            var topicName = _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;

            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[ERROR] EventSet.AddAsync failed: {typeof(T).Name} → {topicName}: {ex.Message}");
            }

            // Phase1変更：新例外型に統一
            throw new KsqlDsl.Communication.KafkaProducerManagerException(
                $"Failed to send {typeof(T).Name} to topic '{topicName}' using new ProducerManager", ex);
        }
    }

    /// <summary>
    /// Phase1変更：新ProducerManager使用
    /// 複数エンティティ一括追加（バッチProducer送信）
    /// </summary>
    public async Task AddRangeAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        if (entities == null)
            throw new ArgumentNullException(nameof(entities));

        var entityList = entities.ToList();
        if (entityList.Count == 0)
            return;

        // 全エンティティの事前バリデーション
        foreach (var entity in entityList)
        {
            ValidateEntity(entity);
        }

        try
        {
            // Phase1変更：旧ProducerService → 新ProducerManager使用（バッチ最適化）
            var producerManager = _context.GetProducerManager();

            var batchContext = new KsqlDsl.Communication.KafkaMessageContext
            {
                MessageId = Guid.NewGuid().ToString(),
                Tags = new Dictionary<string, object>
                {
                    ["entity_type"] = typeof(T).Name,
                    ["method"] = "AddRangeAsync",
                    ["batch_size"] = entityList.Count
                }
            };

            // Phase1変更：バッチ送信最適化メソッド使用
            var batchResult = await producerManager.SendBatchOptimizedAsync(entityList, batchContext, cancellationToken);

            if (_context.Options.EnableDebugLogging)
            {
                var topicName = _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;
                Console.WriteLine($"[DEBUG] EventSet.AddRangeAsync: {entityList.Count}件の{typeof(T).Name} → Topic: {topicName} " +
                                $"(成功: {batchResult.SuccessfulCount}/{batchResult.TotalMessages}) - Phase1: 新ProducerManager使用");
            }

            // バッチ送信の部分失敗チェック
            if (!batchResult.AllSuccessful)
            {
                var failureDetails = string.Join(", ",
                    batchResult.Errors.Select(e => $"Index {e.MessageIndex}: {e.Error?.Reason}"));

                throw new KsqlDsl.Communication.KafkaBatchSendException(
                    $"Batch send partially failed: {batchResult.FailedCount}/{entityList.Count} messages failed. Details: {failureDetails}",
                    batchResult);
            }
        }
        catch (Exception ex) when (!(ex is KsqlDsl.Communication.KafkaBatchSendException))
        {
            var topicName = _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;

            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[ERROR] EventSet.AddRangeAsync failed: {entityList.Count}件の{typeof(T).Name} → {topicName}: {ex.Message}");
            }

            // Phase1変更：新例外型に統一
            throw new KsqlDsl.Communication.KafkaProducerManagerException(
                $"Failed to send batch of {entityList.Count} {typeof(T).Name} to topic '{topicName}' using new ProducerManager", ex);
        }
    }

    // Phase1変更：Consumer関連は次のPhaseで対応予定のため、現状維持
    // ToList/ToListAsync/ForEachAsync などのConsumer関連メソッドは既存実装を維持

    /// <summary>
    /// Consumer関連メソッド（Phase1では変更なし、Phase2で対応予定）
    /// </summary>
    public List<T> ToList()
    {
        // Phase1：Consumer系は既存実装維持（KafkaConsumerServiceの呼び出し）
        // Phase2でKafkaConsumerManagerに移行予定

        var topicName = _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;

        ValidateQueryBeforeExecution();
        var ksqlQuery = ToKsql(isPullQuery: true);

        if (_context.Options.EnableDebugLogging)
        {
            Console.WriteLine($"[DEBUG] EventSet.ToList: {typeof(T).Name} ← Topic: {topicName}");
            Console.WriteLine($"[DEBUG] Generated KSQL: {ksqlQuery}");
            Console.WriteLine($"[WARNING] Phase1: Consumer系は既存実装維持（Phase2で新ConsumerManager移行予定）");
        }

        // Phase1：既存Consumer使用（廃止予定警告付き）
        try
        {
            var consumerService = _context.GetConsumerService(); // 廃止予定メソッド使用
            var results = consumerService.Query<T>(ksqlQuery, _entityModel);
            ValidateQueryResults(results);

            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[DEBUG] Query completed successfully. Results: {results.Count} items");
            }

            return results;
        }
        catch (NotSupportedException ex)
        {
            // Phase1：旧Serviceが廃止された場合の代替処理
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[INFO] 旧ConsumerServiceが廃止されました。Phase2完了後に新ConsumerManagerを使用してください。");
            }

            throw new InvalidOperationException(
                $"Consumer機能は現在移行中です。Phase2完了後に新しいAPIを使用してください。原因: {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[DEBUG] Consumer query error: {ex.Message}");
            }

            throw new InvalidOperationException(
                $"Failed to query topic '{topicName}' for {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    // その他のConsumer関連メソッド（ToListAsync, ForEachAsync等）も同様に
    // Phase1では既存実装維持、Phase2で新ConsumerManager移行予定

    /// <summary>
    /// Phase1移行状況の診断情報取得
    /// </summary>
    public string GetPhase1MigrationStatus()
    {
        return $@"EventSet<{typeof(T).Name}> Phase1移行状況:
✅ Producer系: 新ProducerManager使用に移行完了
   - AddAsync(): 新ProducerManager.GetProducerAsync<T>()使用
   - AddRangeAsync(): 新ProducerManager.SendBatchOptimizedAsync()使用
⚠️  Consumer系: 既存実装維持（Phase2で移行予定）
   - ToList/ToListAsync: 旧ConsumerService使用継続
   - ForEachAsync: 旧ConsumerService使用継続
📋 トピック: {_entityModel.TopicAttribute?.TopicName ?? typeof(T).Name}";
    }

    // 既存のprivateメソッド（ValidateEntity, ValidateQueryBeforeExecution等）は変更なし
    // ... 他のメソッドは既存実装維持
}