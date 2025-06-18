using System;
using System.Collections.Generic;

namespace KsqlDsl.Core.Abstractions;


public class KafkaMessageContext
{
    public string? MessageId { get; set; }
    public string? CorrelationId { get; set; }
    public int? TargetPartition { get; set; }
    public Dictionary<string, object> Headers { get; set; } = new();
    public TimeSpan? Timeout { get; set; }
    public Dictionary<string, object> Tags { get; set; } = new();

    // OpenTelemetry連携
    public System.Diagnostics.ActivityContext? ActivityContext { get; set; }
}
