using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions.Models;


/// <summary>
/// Kafka通信メトリクス
/// 設計理由：既存AvroMetricsとの統合、標準メトリクス提供
/// </summary>
public static class KafkaMetrics
{
    private static readonly System.Diagnostics.Metrics.Meter _meter =
        new("KsqlDsl.Communication", "1.0.0");

    // カウンター
    private static readonly System.Diagnostics.Metrics.Counter<long> _messagesSent =
        _meter.CreateCounter<long>("kafka_messages_sent_total");
    private static readonly System.Diagnostics.Metrics.Counter<long> _messagesReceived =
        _meter.CreateCounter<long>("kafka_messages_received_total");
    private static readonly System.Diagnostics.Metrics.Counter<long> _batchesSent =
        _meter.CreateCounter<long>("kafka_batches_sent_total");

    // ヒストグラム
    private static readonly System.Diagnostics.Metrics.Histogram<double> _sendLatency =
        _meter.CreateHistogram<double>("kafka_send_latency_ms", "ms");
    private static readonly System.Diagnostics.Metrics.Histogram<double> _processingTime =
        _meter.CreateHistogram<double>("kafka_processing_time_ms", "ms");

    public static void RecordMessageSent(string topic, string entityType, bool success, TimeSpan duration)
    {
        _messagesSent.Add(1,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("entity_type", entityType),
            new KeyValuePair<string, object?>("success", success));

        _sendLatency.Record(duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("entity_type", entityType));
    }

    public static void RecordBatchSent(string topic, int messageCount, bool success, TimeSpan duration)
    {
        _batchesSent.Add(1,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("success", success));

        _messagesSent.Add(messageCount,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("batch", true),
            new KeyValuePair<string, object?>("success", success));
    }

    public static void RecordMessageReceived(string topic, string entityType, TimeSpan processingTime)
    {
        _messagesReceived.Add(1,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("entity_type", entityType));

        _processingTime.Record(processingTime.TotalMilliseconds,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("entity_type", entityType));
    }

    public static void RecordThroughput(string direction, string topic, long bytesPerSecond)
    {
        // 実装では適切なメトリクスを記録
    }

    public static void RecordSerializationError(string entityType, string errorType)
    {
        // 実装では適切なメトリクスを記録
    }

    public static void RecordConnectionError(string brokerHost, string errorType)
    {
        // 実装では適切なメトリクスを記録
    }
}