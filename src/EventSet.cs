
using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl;

/// <summary>
/// Core層IEntitySet<T>実装（Phase3 簡素化ログフリー版）
/// 設計理由：Core抽象化との統合、必要最小限の機能のみ
/// 制約：完全ログフリー、副作用排除、複雑性削減
/// </summary>
public class EventSet<T> : IEntitySet<T> where T : class
{
    private readonly IKafkaContext _context;
    private readonly EntityModel _entityModel;

    public EventSet(IKafkaContext context, EntityModel entityModel)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
    }

    // ===============================
    // IEntitySet<T> Core interface implementation
    // ===============================

    /// <summary>
    /// 単一エンティティ非同期送信（簡素化版）
    /// </summary>
    public async Task AddAsync(T entity, CancellationToken cancellationToken = default)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        ValidateEntity(entity);

        try
        {
            // Core層抽象化：実装は具象クラスで定義
            await SendEntityAsync(entity, cancellationToken);

            // ❌ ログ出力削除: Console.WriteLine等一切なし
        }
        catch (Exception ex)
        {
            var topicName = GetTopicName();
            // ❌ ログ出力削除: デバッグログなし
            throw new InvalidOperationException($"Failed to send {typeof(T).Name} to topic '{topicName}'", ex);
        }
    }

    /// <summary>
    /// 非同期リスト取得（簡素化版）
    /// </summary>
    public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var topicName = GetTopicName();
        ValidateQueryBeforeExecution();

        // ❌ ログ出力削除: デバッグログなし

        try
        {
            var results = await ExecuteQueryAsync(cancellationToken);
            ValidateQueryResults(results);

            // ❌ ログ出力削除: 完了ログなし

            return results;
        }
        catch (Exception ex)
        {
            // ❌ ログ出力削除: エラーログなし
            throw new InvalidOperationException($"Failed to query topic '{topicName}' for {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 非同期ForEach処理（ストリーミング統一版）
    /// </summary>
    public async Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        using var timeoutCts = timeout == default ? null : new CancellationTokenSource(timeout);
        using var combinedCts = timeoutCts == null
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        await foreach (var item in this.WithCancellation(combinedCts.Token))
        {
            await action(item);
        }
    }

    // ===============================
    // Metadata Access - 純粋関数
    // ===============================

    /// <summary>
    /// トピック名取得（純粋関数）
    /// </summary>
    public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;

    /// <summary>
    /// エンティティモデル取得（純粋関数）
    /// </summary>
    public EntityModel GetEntityModel() => _entityModel;

    /// <summary>
    /// コンテキスト取得（純粋関数）
    /// </summary>
    public IKafkaContext GetContext() => _context;

    // ===============================
    // IAsyncEnumerable<T> implementation
    // ===============================

    /// <summary>
    /// 非同期列挙子取得
    /// </summary>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var results = await ToListAsync(cancellationToken);
        foreach (var item in results)
        {
            yield return item;
        }
    }

    // ===============================
    // 抽象メソッド（上位層で実装）
    // ===============================

    /// <summary>
    /// 抽象：単一エンティティ送信実装
    /// 実装：上位層で具象Producer呼び出し
    /// 制約：ログ出力・メトリクス収集一切禁止
    /// </summary>
    protected virtual async Task SendEntityAsync(T entity, CancellationToken cancellationToken)
    {
        // デフォルト実装：上位層でオーバーライド必須
        await Task.CompletedTask;
        throw new NotImplementedException("SendEntityAsync must be implemented by concrete class");
    }

    /// <summary>
    /// 抽象：クエリ実行実装
    /// 実装：上位層で具象Consumer呼び出し  
    /// 制約：ログ出力・メトリクス収集一切禁止
    /// </summary>
    protected virtual async Task<List<T>> ExecuteQueryAsync(CancellationToken cancellationToken)
    {
        // デフォルト実装：上位層でオーバーライド必須
        await Task.CompletedTask;
        throw new NotImplementedException("ExecuteQueryAsync must be implemented by concrete class");
    }

    // ===============================
    // ヘルパーメソッド（純粋関数）
    // ===============================

    /// <summary>
    /// エンティティ検証（純粋関数）
    /// </summary>
    private void ValidateEntity(T entity)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        // Core層での基本検証のみ
        var entityType = entity.GetType();
        if (entityType != typeof(T))
        {
            throw new ArgumentException($"Entity type mismatch: expected {typeof(T).Name}, got {entityType.Name}");
        }
    }

    /// <summary>
    /// クエリ実行前検証（純粋関数）
    /// </summary>
    private void ValidateQueryBeforeExecution()
    {
        // クエリ実行前の検証ロジック
        if (_entityModel.TopicAttribute == null)
        {
            throw new InvalidOperationException($"Entity {typeof(T).Name} does not have [Topic] attribute");
        }
    }

    /// <summary>
    /// クエリ結果検証（純粋関数）
    /// </summary>
    private void ValidateQueryResults(List<T> results)
    {
        // クエリ結果の検証ロジック
        if (results == null)
        {
            throw new InvalidOperationException("Query execution returned null results");
        }
    }

    /// <summary>
    /// デバッグ情報（純粋関数、副作用なし）
    /// </summary>
    public override string ToString()
    {
        var topicName = GetTopicName();
        var keyCount = _entityModel.KeyProperties.Length;
        return $"EventSet<{typeof(T).Name}> → Topic: {topicName}, Keys: {keyCount} [Phase3簡素化]";
    }
}