using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions
{
    /// <summary>
    /// クエリ・更新共通操作の統一インターフェース
    /// LINQ互換性を維持
    /// </summary>
    public interface IEntitySet<T> : IQueryable<T>, IAsyncEnumerable<T> where T : class
    {
        // Producer operations
        Task AddAsync(T entity, CancellationToken cancellationToken = default);
        Task AddRangeAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);

        // Consumer operations
        List<T> ToList();
        Task<List<T>> ToListAsync(CancellationToken cancellationToken = default);
        string ToKsql(bool isPullQuery = false);

        // Streaming operations
        void Subscribe(Action<T> onNext, CancellationToken cancellationToken = default);
        Task SubscribeAsync(Func<T, Task> onNext, CancellationToken cancellationToken = default);
        Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default);

        // LINQ extensions
        IEntitySet<T> Where(Expression<Func<T, bool>> predicate);
        IEntitySet<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class;
        IEntitySet<IGrouping<TKey, T>> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector);
        IEntitySet<T> Take(int count);
        IEntitySet<T> Skip(int count);

        // Metadata
        string GetTopicName();
        EntityModel GetEntityModel();
        IKafkaContext GetContext();
    }
}