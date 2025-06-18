using KsqlDsl.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Metrics;


/// <summary>
/// Consumerプール専用メトリクス
/// </summary>
public class ConsumerPoolMetrics
{
    public ConsumerKey ConsumerKey { get; set; } = default!;
    public long CreatedCount { get; set; }
    public long CreationFailures { get; set; }
    public long RentCount { get; set; }
    public long ReturnCount { get; set; }
    public long DiscardedCount { get; set; }
    public long DisposedCount { get; set; }
    public int ActiveConsumers { get; set; }
    public long RebalanceFailures { get; set; }
    public DateTime LastDisposalTime { get; set; }
    public string? LastDisposalReason { get; set; }
    public double FailureRate => CreatedCount > 0 ? (double)CreationFailures / CreatedCount : 0;
}