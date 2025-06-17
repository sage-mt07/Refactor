using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions
{
    /// <summary>
    /// プール制御統一インターフェース
    /// Producer/Consumer両対応
    /// </summary>
    public interface IPoolManager<TKey, TItem> : IDisposable where TKey : notnull
    {
        TItem RentItem(TKey key);
        void ReturnItem(TKey key, TItem item);

        int GetActiveItemCount();
        PoolStatistics GetStatistics();
        Task<PoolHealthStatus> GetHealthAsync();

        void OptimizePools();
        void TrimExcess();
    }

    public class PoolStatistics
    {
        public int TotalPools { get; set; }
        public int ActiveItems { get; set; }
        public int PooledItems { get; set; }
        public long TotalRentals { get; set; }
        public long TotalReturns { get; set; }
        public long TotalDiscarded { get; set; }
        public double UtilizationRate { get; set; }
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
    }

    public class PoolHealthStatus
    {
        public PoolHealthLevel Level { get; set; }
        public List<PoolHealthIssue> Issues { get; set; } = new();
        public PoolStatistics Statistics { get; set; } = new();
        public DateTime CheckedAt { get; set; } = DateTime.UtcNow;
    }

    public enum PoolHealthLevel
    {
        Healthy,
        Warning,
        Critical
    }

    public class PoolHealthIssue
    {
        public string Description { get; set; } = string.Empty;
        public PoolIssueSeverity Severity { get; set; }
        public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
    }

    public enum PoolIssueSeverity
    {
        Low,
        Medium,
        High,
        Critical
    }
}
