﻿using KsqlDsl;
using KsqlDsl.Application;
using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Query.Pipeline;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl;

    /// <summary>
    /// Core層IEntitySet<T>実装
    /// 設計理由：Core抽象化との統合、既存API互換性維持
    /// </summary>
    public class EventSet<T> : IEntitySet<T> where T : class
    {
        private readonly IKafkaContext _context;
        private readonly EntityModel _entityModel;
        private readonly Expression _expression;
        private readonly IQueryProvider _provider;

        public EventSet(IKafkaContext context, EntityModel entityModel)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _expression = Expression.Constant(this);
            _provider = new SimpleQueryProvider();
    }

        public EventSet(IKafkaContext context, EntityModel entityModel, Expression expression)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
            _provider = new SimpleQueryProvider();
        }

        // IQueryable<T> implementation
        public Type ElementType => typeof(T);
        public Expression Expression => _expression;
        public IQueryProvider Provider => _provider;

        // IEntitySet<T> Core interface implementation
        public async Task AddAsync(T entity, CancellationToken cancellationToken = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            ValidateEntity(entity);

            try
            {
                // Core層抽象化：実装は具象クラスで定義
                await SendEntityAsync(entity, cancellationToken);

                if (ShouldLogDebug())
                {
                    var topicName = GetTopicName();
                    Console.WriteLine($"[DEBUG] EventSet.AddAsync: {typeof(T).Name} → Topic: {topicName} - Core層統合");
                }
            }
            catch (Exception ex)
            {
                var topicName = GetTopicName();
                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[ERROR] EventSet.AddAsync failed: {typeof(T).Name} → {topicName}: {ex.Message}");
                }
                throw new InvalidOperationException($"Failed to send {typeof(T).Name} to topic '{topicName}'", ex);
            }
        }

        public async Task AddRangeAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            var entityList = entities.ToList();
            if (entityList.Count == 0)
                return;

            foreach (var entity in entityList)
            {
                ValidateEntity(entity);
            }

            try
            {
                await SendEntitiesAsync(entityList, cancellationToken);

                if (ShouldLogDebug())
                {
                    var topicName = GetTopicName();
                    Console.WriteLine($"[DEBUG] EventSet.AddRangeAsync: {entityList.Count}件の{typeof(T).Name} → Topic: {topicName} - Core層統合");
                }
            }
            catch (Exception ex)
            {
                var topicName = GetTopicName();
                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[ERROR] EventSet.AddRangeAsync failed: {entityList.Count}件の{typeof(T).Name} → {topicName}: {ex.Message}");
                }
                throw new InvalidOperationException($"Failed to send batch of {entityList.Count} {typeof(T).Name} to topic '{topicName}'", ex);
            }
        }

        public List<T> ToList()
        {
            var topicName = GetTopicName();
            ValidateQueryBeforeExecution();
            var ksqlQuery = ToKsql(isPullQuery: true);

            if (ShouldLogDebug())
            {
                Console.WriteLine($"[DEBUG] EventSet.ToList: {typeof(T).Name} ← Topic: {topicName} - Core層統合");
                Console.WriteLine($"[DEBUG] Generated KSQL: {ksqlQuery}");
            }

            try
            {
                var results = ExecuteQuery(ksqlQuery);
                ValidateQueryResults(results);

                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[DEBUG] Query completed successfully. Results: {results.Count} items");
                }

                return results;
            }
            catch (Exception ex)
            {
                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[DEBUG] Consumer query error: {ex.Message}");
                }
                throw new InvalidOperationException($"Failed to query topic '{topicName}' for {typeof(T).Name}: {ex.Message}", ex);
            }
        }

        public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
        {
            await Task.Delay(1, cancellationToken);
            return ToList();
        }

        public string ToKsql(bool isPullQuery = false)
        {
            try
            {
                var topicName = GetTopicName();

                // 簡素化実装：基本的なKSQL生成
                var queryType = isPullQuery ? "SELECT" : "CREATE STREAM";
                var baseQuery = $"{queryType} * FROM {topicName}";

                // 簡単なWhere句解析（将来のQuery層実装まで）
                if (_expression is MethodCallExpression methodCall && methodCall.Method.Name == "Where")
                {
                    baseQuery += " WHERE /* 条件解析は Query層実装後 */";
                }

                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[DEBUG] Generated KSQL: {baseQuery}");
                }

                return baseQuery;
            }
            catch (Exception ex)
            {
                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[DEBUG] KSQL変換エラー: {ex.Message}");
                    Console.WriteLine($"[DEBUG] Expression: {_expression}");
                }
                return $"/* KSQL変換エラー: {ex.Message} */";
            }
        }

        // IEntitySet<T> LINQ Extensions
        public IEntitySet<T> Where(Expression<Func<T, bool>> predicate)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Where),
                new[] { typeof(T) },
                _expression,
                Expression.Quote(predicate));

            return CreateNewEventSet(_context, _entityModel, methodCall);
        }

        public IEntitySet<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Select),
                new[] { typeof(T), typeof(TResult) },
                _expression,
                Expression.Quote(selector));

            // ResultType用のEntityModelを動的作成
            var resultEntityModel = CreateDynamicEntityModel<TResult>();
            return CreateNewEventSet<TResult>(_context, resultEntityModel, methodCall);
        }

        public IEntitySet<IGrouping<TKey, T>> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.GroupBy),
                new[] { typeof(T), typeof(TKey) },
                _expression,
                Expression.Quote(keySelector));

            // IGrouping用のEntityModelを動的作成
            var groupingEntityModel = CreateGroupingEntityModel<TKey>();
            return CreateNewEventSet<IGrouping<TKey, T>>(_context, groupingEntityModel, methodCall);
        }

        public IEntitySet<T> Take(int count)
        {
            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Take),
                new[] { typeof(T) },
                _expression,
                Expression.Constant(count));

            return CreateNewEventSet(_context, _entityModel, methodCall);
        }

        public IEntitySet<T> Skip(int count)
        {
            var methodCall = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Skip),
                new[] { typeof(T) },
                _expression,
                Expression.Constant(count));

            return CreateNewEventSet(_context, _entityModel, methodCall);
        }

        // IEntitySet<T> Streaming Operations
        public void Subscribe(Action<T> onNext, CancellationToken cancellationToken = default)
        {
            if (onNext == null)
                throw new ArgumentNullException(nameof(onNext));

            Task.Run(async () =>
            {
                await foreach (var item in this.WithCancellation(cancellationToken))
                {
                    onNext(item);
                }
            }, cancellationToken);
        }

        public async Task SubscribeAsync(Func<T, Task> onNext, CancellationToken cancellationToken = default)
        {
            if (onNext == null)
                throw new ArgumentNullException(nameof(onNext));

            await foreach (var item in this.WithCancellation(cancellationToken))
            {
                await onNext(item);
            }
        }

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

        // IEntitySet<T> Metadata Access
        public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;
        public EntityModel GetEntityModel() => _entityModel;
        public IKafkaContext GetContext() => _context;

        // IEnumerable<T> implementation
        public IEnumerator<T> GetEnumerator() => ToList().GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        // IAsyncEnumerable<T> implementation
        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            var results = await ToListAsync(cancellationToken);
            foreach (var item in results)
            {
                yield return item;
            }
        }

        // 抽象メソッド（上位層で実装）
        protected virtual async Task SendEntityAsync(T entity, CancellationToken cancellationToken)
        {
            try
            {
                // IKafkaContextをKafkaContextにキャスト
                var kafkaContext = (KafkaContext)_context;
                var producerManager = kafkaContext.GetProducerManager();
                await producerManager.SendAsync(entity, cancellationToken);

                if (ShouldLogDebug())
                {
                    var topicName = GetTopicName();
                    Console.WriteLine($"[DEBUG] EventSet.SendEntityAsync: {typeof(T).Name} → {topicName} - 簡素化統合");
                }
            }
            catch (Exception ex)
            {
                var topicName = GetTopicName();
                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[ERROR] EventSet.SendEntityAsync failed: {typeof(T).Name} → {topicName}: {ex.Message}");
                }
                throw new InvalidOperationException($"Failed to send {typeof(T).Name} to topic '{topicName}'", ex);
            }
        }
        protected virtual async Task SendEntitiesAsync(IEnumerable<T> entities, CancellationToken cancellationToken)
        {
            try
            {
                // 簡素化：直接ProducerManagerを使用
                var kafkaContext = (KafkaContext)_context;
                var producerManager = kafkaContext.GetProducerManager();
                await producerManager.SendRangeAsync(entities, cancellationToken);

                if (ShouldLogDebug())
                {
                    var topicName = GetTopicName();
                    var count = entities.Count();
                    Console.WriteLine($"[DEBUG] EventSet.SendEntitiesAsync: {count}件の{typeof(T).Name} → {topicName} - 簡素化統合");
                }
            }
            catch (Exception ex)
            {
                var topicName = GetTopicName();
                var count = entities.Count();
                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[ERROR] EventSet.SendEntitiesAsync failed: {count}件の{typeof(T).Name} → {topicName}: {ex.Message}");
                }
                throw new InvalidOperationException($"Failed to send batch of {count} {typeof(T).Name} to topic '{topicName}'", ex);
            }
        }

        protected virtual List<T> ExecuteQuery(string ksqlQuery)
        {
            try
            {
                var kafkaContext = (KafkaContext)_context;
                var consumerManager = kafkaContext.GetConsumerManager();

                // Pull Query風の実装（簡略化）
                var options = new KafkaFetchOptions
                {
                    MaxRecords = 100,
                    Timeout = TimeSpan.FromSeconds(30)
                };

                var results = consumerManager.FetchAsync<T>(options, CancellationToken.None).GetAwaiter().GetResult();

                if (ShouldLogDebug())
                {
                    var topicName = GetTopicName();
                    Console.WriteLine($"[INFO] EventSet.ExecuteQuery: {results.Count}件取得 {typeof(T).Name} ← {topicName} - 簡素化統合");
                    Console.WriteLine($"[DEBUG] Generated KSQL: {ksqlQuery}");
                }

                return results;
            }
            catch (Exception ex)
            {
                var topicName = GetTopicName();
                if (ShouldLogDebug())
                {
                    Console.WriteLine($"[ERROR] EventSet.ExecuteQuery failed: {typeof(T).Name} ← {topicName}: {ex.Message}");
                }
                throw new InvalidOperationException($"Failed to query topic '{topicName}' for {typeof(T).Name}: {ex.Message}", ex);
            }
        }

        // Factory Methods - 具象クラスでオーバーライド
        protected virtual IEntitySet<T> CreateNewEventSet(IKafkaContext context, EntityModel entityModel, Expression expression)
        {
            return new EventSet<T>(context, entityModel, expression);
        }

        protected virtual IEntitySet<TResult> CreateNewEventSet<TResult>(IKafkaContext context, EntityModel entityModel, Expression expression) where TResult : class
        {
            return new EventSet<TResult>(context, entityModel, expression);
        }

        // ヘルパーメソッド
        private void ValidateEntity(T entity)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            // Core層での基本検証
            var entityType = entity.GetType();
            if (entityType != typeof(T))
            {
                throw new ArgumentException($"Entity type mismatch: expected {typeof(T).Name}, got {entityType.Name}");
            }
        }

        private void ValidateQueryBeforeExecution()
        {
            // クエリ実行前の検証ロジック
            if (_entityModel.TopicAttribute == null)
            {
                throw new InvalidOperationException($"Entity {typeof(T).Name} does not have [Topic] attribute");
            }
        }

        private void ValidateQueryResults(List<T> results)
        {
            // クエリ結果の検証ロジック
            if (results == null)
            {
                throw new InvalidOperationException("Query execution returned null results");
            }
        }

        private bool ShouldLogDebug()
        {
            // デバッグログ出力判定
            try
            {
                // KafkaContextから設定を取得
                return true; // 簡略実装
            }
            catch
            {
                return false;
            }
        }

        private EntityModel CreateDynamicEntityModel<TResult>()
        {
            // 動的EntityModel作成（投影用）
            return new EntityModel
            {
                EntityType = typeof(TResult),
                AllProperties = typeof(TResult).GetProperties(),
                KeyProperties = new System.Reflection.PropertyInfo[0]
            };
        }

        private EntityModel CreateGroupingEntityModel<TKey>()
        {
            // GroupBy用EntityModel作成
            return new EntityModel
            {
                EntityType = typeof(IGrouping<TKey, T>),
                AllProperties = typeof(IGrouping<TKey, T>).GetProperties(),
                KeyProperties = new System.Reflection.PropertyInfo[0]
            };
        }

        /// <summary>
        /// Core層移行状況の診断情報取得
        /// </summary>
        public string GetCoreMigrationStatus()
        {
            return $@"EventSet<{typeof(T).Name}> Core層統合状況:
✅ Core.Abstractions: IEntitySet<T>実装完了
✅ Core.Context: IKafkaContext統合完了  
✅ Core.Modeling: EntityModel統合完了
🔄 Producer系: 抽象化対応（具象実装待ち）
🔄 Consumer系: 抽象化対応（具象実装待ち）
📋 トピック: {_entityModel.TopicAttribute?.TopicName ?? typeof(T).Name}
📋 キー数: {_entityModel.KeyProperties.Length}
📋 プロパティ数: {_entityModel.AllProperties.Length}";
        }

        public override string ToString()
        {
            var topicName = GetTopicName();
            var keyCount = _entityModel.KeyProperties.Length;
            return $"EventSet<{typeof(T).Name}> → Topic: {topicName}, Keys: {keyCount} [Core層統合]";
        }

    // 簡素化QueryProvider追加
    private class SimpleQueryProvider : IQueryProvider
    {
        public IQueryable CreateQuery(Expression expression)
        {
            throw new NotSupportedException("Generic CreateQuery not supported");
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression) 
        {
            // TODO: Phase 2統合時に実装
            // - Query層のQueryExecutionPipelineと統合
            // - 適切な依存関係注入の設計
            // - EventSet.csコンストラクタの見直し
            throw new NotSupportedException(
                "Complex LINQ queries require ToList() or ToListAsync(). " +
                "Full Query layer integration will be implemented in Phase 2.");
        }

        public object Execute(Expression expression)
        {
            throw new NotSupportedException("Execute not supported");
        }

        public TResult Execute<TResult>(Expression expression)
        {
            throw new NotSupportedException("Execute not supported");
        }
    }
}


