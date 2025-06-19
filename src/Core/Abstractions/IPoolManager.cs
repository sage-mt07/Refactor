using KsqlDsl.Messaging.Abstractions;
using KsqlDsl.Monitoring.Abstractions.Models;
using System;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions;

/// <summary>
/// プール制御統一インターフェース
/// Producer/Consumer両対応
/// </summary>
public interface IPoolManager<TKey, TItem> : IDisposable where TKey : notnull
{
    TItem RentResource(TKey key);
    TItem RentItem(TKey key);
    void ReturnItem(TKey key, TItem item);

    int GetActiveItemCount();
    PoolStatistics GetStatistics();
    Task<PoolHealthStatus> GetHealthAsync();

    void OptimizePools();
    void TrimExcess();
}
