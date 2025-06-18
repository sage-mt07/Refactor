using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Producers.Core;
// =============================================================================
// Pool & Internal Classes - プール・内部管理クラス
// =============================================================================

/// <summary>
/// プールされたProducer
/// </summary>
public class PooledProducer
{
    public IProducer<object, object> Producer { get; set; } = default!;
    public DateTime CreatedAt { get; set; }
    public DateTime LastUsed { get; set; }
    public int UsageCount { get; set; }
    public bool IsHealthy { get; set; } = true;
}