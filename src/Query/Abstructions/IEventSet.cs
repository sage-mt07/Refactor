using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Query.Abstractions
{
    /// <summary>
    /// EventSet操作の共通インターフェース
    /// 設計理由：EventSet分割に対する統一API
    /// </summary>
    public interface IEventSet<T> : IQueryable<T>, IAsyncEnumerable<T> where T : class
    {
        // Core Operations
        Task AddAsync(T entity, CancellationToken cancellationToken = default);
        Task AddRangeAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);

        // Query Operations  
        List<T> ToList();
        Task<List<T>> ToListAsync(CancellationToken cancellationToken = default);
        string ToKsql(bool isPullQuery = false);

        // Streaming Operations
        void Subscribe(Action<T> onNext, CancellationToken cancellationToken = default);
        Task SubscribeAsync(Func<T, Task> onNext, CancellationToken cancellationToken = default);
        Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default);

        // LINQ Extensions
        IEventSet<T> Where(Expression<Func<T, bool>> predicate);
        IEventSet<TResult> Select<TResult>(Expression<Func<T, TResult>> selector);
        IEventSet<IGrouping<TKey, T>> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector);
        IEventSet<T> Take(int count);
        IEventSet<T> Skip(int count);

        // Metadata Access
        string GetTopicName();
        KsqlDsl.Modeling.EntityModel GetEntityModel();
        KafkaContext GetContext();
    }
}