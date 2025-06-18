using KsqlDsl.Messaging.Producers.Exception;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Messaging.Producers.Core;

/// <summary>
/// バッチ送信結果
/// </summary>
public class KafkaBatchDeliveryResult
{
    public string Topic { get; set; } = string.Empty;
    public int TotalMessages { get; set; }
    public int SuccessfulCount { get; set; }
    public int FailedCount { get; set; }
    public bool AllSuccessful => FailedCount == 0;
    public List<KafkaDeliveryResult> Results { get; set; } = new();
    public List<BatchDeliveryError> Errors { get; set; } = new();
    public TimeSpan TotalLatency { get; set; }
}
