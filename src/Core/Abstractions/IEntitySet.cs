using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions;

/// <summary>
/// クエリ・更新共通操作の統一インターフェース
/// LINQ互換性を維持
/// </summary>
public interface IEntitySet<T> :  IAsyncEnumerable<T> where T : class
{
    // Producer operations
    Task AddAsync(T entity, CancellationToken cancellationToken = default);

    // Consumer operations
    Task<List<T>> ToListAsync(CancellationToken cancellationToken = default);

    // Streaming operations
    Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default);



    // Metadata
    string GetTopicName();
    EntityModel GetEntityModel();
    IKafkaContext GetContext();
}