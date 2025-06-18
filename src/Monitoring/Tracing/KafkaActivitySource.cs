using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Tracing;

// =============================================================================
// Activity Source & Metrics - 監視・計測
// =============================================================================

/// <summary>
/// Kafka通信用ActivitySource
/// 設計理由：OpenTelemetry分散トレーシングとの統合
/// </summary>
public static class KafkaActivitySource
{
    private static readonly System.Diagnostics.ActivitySource _activitySource =
        new("KsqlDsl.Communication", "1.0.0");

    public static System.Diagnostics.Activity? StartActivity(string name) =>
        _activitySource.StartActivity(name);
}