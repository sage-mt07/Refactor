using KsqlDsl.Core.Modeling;
using KsqlDsl.Messaging.Consumers;
using KsqlDsl.Messaging.Consumers.Exceptions;
using KsqlDsl.Query.Abstractions;
using KsqlDsl.Query.Translation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Query.EventSets
{
    /// <summary>
    /// EventSet Push/Pull型操作、EMIT制御
    /// 設計理由：ストリーミング関連責務の分離
    /// </summary>
    public partial class EventSetStreaming<T> : EventSetCore<T> where T : class
    {
        private readonly IQueryTranslator _queryTranslator;

        internal EventSetStreaming(KafkaContext context, EntityModel entityModel)
            : base(context, entityModel)
        {
            _queryTranslator = new QueryTranslator();
        }

        internal EventSetStreaming(KafkaContext context, EntityModel entityModel, Expression expression)
            : base(context, entityModel, expression)
        {
            _queryTranslator = new QueryTranslator();
        }

        // Pull Query実行（ToList系）
       
        public override List<T> ToList()
        {
            var topicName = GetTopicName();
            ValidateQueryBeforeExecution();
            // Pull Queryとして実行（isPullQuery: true）
            var ksqlQuery = ToKsql(isPullQuery: true);
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[DEBUG] EventSetStreaming.ToList: {typeof(T).Name} ← Topic: {topicName}");
                Console.WriteLine($"[DEBUG] Generated KSQL: {ksqlQuery}");
                Console.WriteLine($"[DEBUG] Query Diagnostics: {_queryTranslator.GetDiagnostics()}");
            }

            try
            {
                ValidateKsqlQuery(ksqlQuery);

                // ConsumerManagerを使用（新しいアーキテクチャ）
                var consumerManager = _context.GetConsumerManager();
                var results = ExecuteQueryWithConsumerManager<T>(consumerManager, ksqlQuery);

                ValidateQueryResults(results);
                if (_context.Options.EnableDebugLogging)
                {
                    Console.WriteLine($"[DEBUG] Query completed successfully. Results: {results.Count} items");
                }
                return results;
            }
            catch (Exception ex)
            {
                HandleQueryException(ex, topicName, "ToList");
                throw;
            }
        }

        /// <summary>
        /// クエリ実行前のバリデーション
        /// </summary>
        private void ValidateQueryBeforeExecution()
        {
            if (_entityModel.TopicAttribute == null)
            {
                throw new InvalidOperationException($"Entity {typeof(T).Name} does not have [Topic] attribute");
            }

            if (string.IsNullOrEmpty(_entityModel.TopicAttribute.TopicName))
            {
                throw new InvalidOperationException($"Entity {typeof(T).Name} has invalid topic name");
            }
        }

        /// <summary>
        /// KSQLクエリのバリデーション
        /// </summary>
        private void ValidateKsqlQuery(string ksqlQuery)
        {
            if (string.IsNullOrWhiteSpace(ksqlQuery))
            {
                throw new InvalidOperationException("Generated KSQL query is empty or invalid");
            }

            if (ksqlQuery.Contains("/* KSQL変換エラー"))
            {
                throw new InvalidOperationException($"KSQL translation error detected: {ksqlQuery}");
            }
        }

        /// <summary>
        /// クエリ結果のバリデーション
        /// </summary>
        private void ValidateQueryResults(List<T> results)
        {
            if (results == null)
            {
                throw new InvalidOperationException("Query execution returned null results");
            }

            // 結果の妥当性チェック（必要に応じて追加）
            foreach (var result in results)
            {
                if (result == null)
                {
                    throw new InvalidOperationException("Query returned null entity in results");
                }
            }
        }

        /// <summary>
        /// クエリ例外のハンドリング
        /// </summary>
        private void HandleQueryException(Exception ex, string topicName, string operation)
        {
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[ERROR] EventSetStreaming.{operation} failed: {typeof(T).Name} ← Topic: {topicName}");
                Console.WriteLine($"[ERROR] Exception: {ex.Message}");
                Console.WriteLine($"[ERROR] StackTrace: {ex.StackTrace}");
            }

            // 例外の種類に応じた追加ログ（必要に応じて）
            switch (ex)
            {
                case InvalidOperationException _:
                    Console.WriteLine($"[ERROR] Query validation or execution error for {typeof(T).Name}");
                    break;
                case NotSupportedException _:
                    Console.WriteLine($"[ERROR] Unsupported query operation for {typeof(T).Name}");
                    break;
                default:
                    Console.WriteLine($"[ERROR] Unexpected error during {operation} for {typeof(T).Name}");
                    break;
            }
        }

        /// <summary>
        /// ConsumerManagerを使用したクエリ実行
        /// 設計理由：新しいConsumer architecture対応
        /// </summary>
        private List<T> ExecuteQueryWithConsumerManager<TResult>(KafkaConsumerManager consumerManager, string ksqlQuery) where TResult : class
        {
            // Phase2での完全実装までの一時的な実装
            // TODO: ConsumerManagerでKSQLクエリを実行する機能を実装

            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[WARNING] ExecuteQueryWithConsumerManager is transitioning to new Consumer architecture");
                Console.WriteLine($"[WARNING] KSQL Query: {ksqlQuery}");
                Console.WriteLine($"[WARNING] Returning empty results until Phase2 completion");
            }

            // 現在は空のリストを返す（Phase2で実装予定）
            return new List<T>();
        }
        public override async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
        {
            var topicName = GetTopicName();
            ValidateQueryBeforeExecution();
            var ksqlQuery = ToKsql(isPullQuery: true);
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[DEBUG] EventSetStreaming.ToListAsync: {typeof(T).Name} ← Topic: {topicName}");
                Console.WriteLine($"[DEBUG] Generated KSQL: {ksqlQuery}");
            }

            try
            {
                ValidateKsqlQueryContent(ksqlQuery); // ✅ 統一されたメソッド名を使用

                // ConsumerManagerを使用（新しいアーキテクチャ）
                var consumerManager = _context.GetConsumerManager(); // ✅ GetConsumerService → GetConsumerManager
                var results = await ExecuteQueryWithConsumerManagerAsync<T>(consumerManager, ksqlQuery, cancellationToken);

                ValidateQueryResults(results);

                if (_context.Options.EnableDebugLogging)
                {
                    Console.WriteLine($"[DEBUG] ToListAsync completed successfully. Results: {results.Count} items");
                }

                return results;
            }
            catch (OperationCanceledException)
            {
                if (_context.Options.EnableDebugLogging)
                {
                    Console.WriteLine($"[DEBUG] Query cancelled by CancellationToken");
                }
                throw;
            }
            catch (Exception ex)
            {
                HandleQueryException(ex, topicName, "ToListAsync");
                throw;
            }
        }
        private async Task<List<T>> ExecuteQueryWithConsumerManagerAsync<TResult>(
          KafkaConsumerManager consumerManager,
          string ksqlQuery,
          CancellationToken cancellationToken) where TResult : class
        {
            // Phase2での完全実装までの一時的な実装
            // TODO: ConsumerManagerでKSQLクエリを非同期実行する機能を実装

            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[WARNING] ExecuteQueryWithConsumerManagerAsync is transitioning to new Consumer architecture");
                Console.WriteLine($"[WARNING] Entity: {typeof(TResult).Name}");
                Console.WriteLine($"[WARNING] KSQL Query: {ksqlQuery}");
                Console.WriteLine($"[WARNING] Returning empty results until Phase2 completion");
            }

            // CancellationTokenをチェック
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                // 将来的にはここでConsumerManagerを使用してKSQLクエリを非同期実行
                // 現在は非同期的に空のリストを返す（Phase2で実装予定）

                // 実際の実装例（Phase2で有効化）:
                // var subscriptionOptions = new KafkaSubscriptionOptions 
                // { 
                //     GroupId = $"query-{Guid.NewGuid()}", 
                //     AutoCommit = false 
                // };
                // var consumer = await consumerManager.CreateConsumerAsync<TResult>(subscriptionOptions);
                // var queryResults = await consumer.ExecuteKsqlQueryAsync(ksqlQuery, cancellationToken);
                // return queryResults;

                // 一時的な非同期遅延（実際のクエリ実行をシミュレート）
                await Task.Delay(10, cancellationToken);

                return new List<T>();
            }
            catch (OperationCanceledException)
            {
                if (_context.Options.EnableDebugLogging)
                {
                    Console.WriteLine($"[INFO] ExecuteQueryWithConsumerManagerAsync cancelled for {typeof(TResult).Name}");
                }
                throw; // キャンセル例外は再スロー
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] ExecuteQueryWithConsumerManagerAsync failed: {ex.Message}");
                throw new InvalidOperationException($"Async query execution failed for {typeof(TResult).Name}", ex);
            }
        }

        // Push Query実行（Subscribe系）
        public override void Subscribe(Action<T> onNext, CancellationToken cancellationToken = default)
        {
            if (onNext == null)
                throw new ArgumentNullException(nameof(onNext));

            var topicName = GetTopicName();
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[DEBUG] EventSetStreaming.Subscribe: {typeof(T).Name} ← Topic: {topicName} (Push型購読開始)");
            }
        }

        public override async Task SubscribeAsync(Func<T, Task> onNext, CancellationToken cancellationToken = default)
        {
            if (onNext == null)
                throw new ArgumentNullException(nameof(onNext));

            await Task.Delay(1, cancellationToken);

            var topicName = GetTopicName();
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[DEBUG] EventSetStreaming.SubscribeAsync: {typeof(T).Name} ← Topic: {topicName} (非同期Push型購読開始)");
            }
        }

        public override async Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            var topicName = GetTopicName();
            var timeoutMs = timeout == TimeSpan.Zero ? int.MaxValue : (int)timeout.TotalMilliseconds;

            // Push Query（ストリーミング取得）専用
            var ksqlQuery = ToKsql(isPullQuery: false);

            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[DEBUG] EventSetStreaming.ForEachAsync: {typeof(T).Name} ← Topic: {topicName} (Push型ストリーミング開始)");
                Console.WriteLine($"[DEBUG] Generated KSQL: {ksqlQuery}");
                Console.WriteLine($"[DEBUG] Timeout: {timeoutMs}ms");
            }

            ValidateQueryBeforeExecution();

            var consumerService = _context.GetConsumerService();

            CancellationTokenSource? timeoutCts = null;
            CancellationToken effectiveToken = cancellationToken;

            if (timeoutMs != int.MaxValue)
            {
                timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(timeoutMs);
                effectiveToken = timeoutCts.Token;
            }

            try
            {
                await consumerService.SubscribeStreamAsync<T>(
                    ksqlQuery,
                    _entityModel,
                    async (item) =>
                    {
                        if (effectiveToken.IsCancellationRequested)
                            return;

                        await action(item);
                    },
                    effectiveToken);

                if (_context.Options.EnableDebugLogging)
                {
                    Console.WriteLine($"[DEBUG] ForEachAsync streaming completed for {typeof(T).Name}");
                }
            }
            catch (OperationCanceledException ex) when (timeoutCts?.Token.IsCancellationRequested == true && !cancellationToken.IsCancellationRequested)
            {
                if (_context.Options.EnableDebugLogging)
                {
                    Console.WriteLine($"[DEBUG] ForEachAsync streaming timeout after {timeoutMs}ms for {typeof(T).Name}");
                }
                throw new TimeoutException($"ForEachAsync operation timed out after {timeoutMs}ms", ex);
            }
            catch (Exception ex)
            {
                HandleQueryException(ex, topicName, "ForEachAsync");
                throw;
            }
            finally
            {
                timeoutCts?.Dispose();
            }
        }

        // KSQL変換（フラグ制御版）
        public override string ToKsql(bool isPullQuery = false)
        {
            try
            {
                var topicName = GetTopicName();
                return _queryTranslator.ToKsql(_expression, topicName, isPullQuery);
            }
            catch (Exception ex)
            {
                if (_context.Options.EnableDebugLogging)
                {
                    Console.WriteLine($"[DEBUG] KSQL変換エラー: {ex.Message}");
                    Console.WriteLine($"[DEBUG] Expression: {_expression}");
                }
                return $"/* KSQL変換エラー: {ex.Message} */";
            }
        }

        // LINQ Extensions - 新しいインスタンスを返す
        public override IEventSet<T> Where(Expression<Func<T, bool>> predicate)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Where),
                new[] { typeof(T) },
                _expression,
                Expression.Quote(predicate));

            return new EventSetStreaming<T>(_context, _entityModel, methodCall);
        }

        public override IEventSet<TResult> Select<TResult>(Expression<Func<T, TResult>> selector)where TResult :class
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Select),
                new[] { typeof(T), typeof(TResult) },
                _expression,
                Expression.Quote(selector));

            return new EventSetStreaming<TResult>(_context, _entityModel, methodCall);
        }

        public override IEventSet<IGrouping<TKey, T>> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.GroupBy),
                new[] { typeof(T), typeof(TKey) },
                _expression,
                Expression.Quote(keySelector));

            return new EventSetStreaming<IGrouping<TKey, T>>(_context, _entityModel, methodCall);
        }

        public override IEventSet<T> Take(int count)
        {
            if (count <= 0)
                throw new ArgumentException("Count must be positive", nameof(count));

            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Take),
                new[] { typeof(T) },
                _expression,
                Expression.Constant(count));

            return new EventSetStreaming<T>(_context, _entityModel, methodCall);
        }

        public override IEventSet<T> Skip(int count)
        {
            if (count < 0)
                throw new ArgumentException("Count cannot be negative", nameof(count));

            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Skip),
                new[] { typeof(T) },
                _expression,
                Expression.Constant(count));

            return new EventSetStreaming<T>(_context, _entityModel, methodCall);
        }



    }
}