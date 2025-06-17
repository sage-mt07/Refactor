using KsqlDsl.Core.Modeling;
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

            var consumerService = _context.GetConsumerService();

            try
            {
                ValidateKsqlQuery(ksqlQuery);
                var results = consumerService.Query<T>(ksqlQuery, _entityModel);
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

            var consumerService = _context.GetConsumerService();

            try
            {
                ValidateKsqlQuery(ksqlQuery);
                var results = await consumerService.QueryAsync<T>(ksqlQuery, _entityModel, cancellationToken);
                ValidateQueryResults(results);

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

        public override IEventSet<TResult> Select<TResult>(Expression<Func<T, TResult>> selector)
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

        // プライベートヘルパーメソッド
        private void ValidateKsqlQuery(string ksqlQuery)
        {
            if (string.IsNullOrEmpty(ksqlQuery) || ksqlQuery.Contains("/* KSQL変換エラー"))
            {
                throw new InvalidOperationException($"Failed to generate valid KSQL query for {typeof(T).Name}");
            }
        }

        private void HandleQueryException(Exception ex, string topicName, string operation)
        {
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[DEBUG] {operation} error: {ex.Message}");
            }

            if (ex is KafkaConsumerException)
            {
                throw new InvalidOperationException(
                    $"Failed to execute {operation} on topic '{topicName}' for {typeof(T).Name}: {ex.Message}", ex);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Unexpected error in {operation} for {typeof(T).Name} from topic '{topicName}': {ex.Message}", ex);
            }
        }
    }
}