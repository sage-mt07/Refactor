using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using KsqlDsl.Modeling;
using KsqlDsl.Query.Abstractions;

namespace KsqlDsl.Query.EventSets
{
    /// <summary>
    /// EventSet基本CRUD操作
    /// 設計理由：EventSet.csから基本操作責務を分離
    /// </summary>
    public partial class EventSetCore<T> : IEventSet<T> where T : class
    {
        protected readonly KafkaContext _context;
        protected readonly EntityModel _entityModel;
        protected readonly IQueryProvider _queryProvider;
        protected readonly Expression _expression;

        internal EventSetCore(KafkaContext context, EntityModel entityModel)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _queryProvider = new EventQueryProvider<T>(context, entityModel);
            _expression = Expression.Constant(this);
        }

        internal EventSetCore(KafkaContext context, EntityModel entityModel, Expression expression)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
            _queryProvider = new EventQueryProvider<T>(context, entityModel);
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        // IQueryable実装
        public Type ElementType => typeof(T);
        public Expression Expression => _expression;
        public IQueryProvider Provider => _queryProvider;

        // 基本追加操作
        public async Task AddAsync(T entity, CancellationToken cancellationToken = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            ValidateEntity(entity);

            var producerService = _context.GetProducerService();
            await producerService.SendAsync(entity, _entityModel, cancellationToken);

            if (_context.Options.EnableDebugLogging)
            {
                var topicName = _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;
                Console.WriteLine($"[DEBUG] EventSetCore.AddAsync: {typeof(T).Name} → Topic: {topicName} (送信完了)");
            }
        }

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

            var producerService = _context.GetProducerService();
            await producerService.SendRangeAsync(entityList, _entityModel, cancellationToken);

            if (_context.Options.EnableDebugLogging)
            {
                var topicName = _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;
                Console.WriteLine($"[DEBUG] EventSetCore.AddRangeAsync: {entityList.Count}件の{typeof(T).Name} → Topic: {topicName} (送信完了)");
            }
        }

        // IEnumerable実装
        public IEnumerator<T> GetEnumerator()
        {
            return ToList().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        // IAsyncEnumerable実装
        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.Delay(1, cancellationToken);

            var items = ToList();
            foreach (var item in items)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;

                yield return item;
            }
        }

        // メタデータアクセス
        public EntityModel GetEntityModel() => _entityModel;
        public KafkaContext GetContext() => _context;
        public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name;

        // バリデーション（共通）
        protected virtual void ValidateEntity(T entity)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            // 基本的なキープロパティチェック
            if (_entityModel.KeyProperties.Length > 0)
            {
                foreach (var keyProperty in _entityModel.KeyProperties)
                {
                    var keyValue = keyProperty.GetValue(entity);
                    if (keyValue == null)
                    {
                        throw new InvalidOperationException(
                            $"Key property '{keyProperty.Name}' cannot be null for entity type '{typeof(T).Name}'");
                    }

                    if (keyProperty.PropertyType == typeof(string) && string.IsNullOrEmpty((string)keyValue))
                    {
                        throw new InvalidOperationException(
                            $"Key property '{keyProperty.Name}' cannot be empty for entity type '{typeof(T).Name}'");
                    }
                }
            }
        }

        public override string ToString()
        {
            var topicName = GetTopicName();
            var entityName = typeof(T).Name;
            return $"EventSetCore<{entityName}> → Topic: {topicName}";
        }

        // 抽象メソッド（他の部分クラスで実装）
        public abstract List<T> ToList();
        public abstract Task<List<T>> ToListAsync(CancellationToken cancellationToken = default);
        public abstract string ToKsql(bool isPullQuery = false);
        public abstract void Subscribe(Action<T> onNext, CancellationToken cancellationToken = default);
        public abstract Task SubscribeAsync(Func<T, Task> onNext, CancellationToken cancellationToken = default);
        public abstract Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default);
        public abstract IEventSet<T> Where(Expression<Func<T, bool>> predicate);
        public abstract IEventSet<TResult> Select<TResult>(Expression<Func<T, TResult>> selector);
        public abstract IEventSet<IGrouping<TKey, T>> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector);
        public abstract IEventSet<T> Take(int count);
        public abstract IEventSet<T> Skip(int count);
    }
}