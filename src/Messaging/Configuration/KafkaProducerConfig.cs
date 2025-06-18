using KsqlDsl.Configuration.Options;
using System;
using System.Collections.Generic;
namespace KsqlDsl.Messaging.Configuration;
/// <summary>
/// Producer設定
/// </summary>
public class KafkaProducerConfig
{
    // 基本設定
    public string BootstrapServers { get; set; } = "localhost:9092";
    public Acks Acks { get; set; } = Acks.All;
    public bool EnableIdempotence { get; set; } = true;
    public int MaxInFlight { get; set; } = 1;
    public CompressionType CompressionType { get; set; } = CompressionType.Snappy;

    // パフォーマンス設定
    public int LingerMs { get; set; } = 5;
    public int BatchSize { get; set; } = 16384;
    public int RequestTimeoutMs { get; set; } = 30000;
    public int DeliveryTimeoutMs { get; set; } = 120000;
    public int RetryBackoffMs { get; set; } = 100;

    // セキュリティ設定
    public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.Plaintext;
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }

    // 追加設定
    public Dictionary<string, object> AdditionalConfig { get; set; } = new();

    // ヘルス閾値
    public ProducerHealthThresholds HealthThresholds { get; set; } = new();

    public int GetKeyHash()
    {
        return HashCode.Combine(
            BootstrapServers, Acks, EnableIdempotence, MaxInFlight,
            CompressionType, LingerMs, BatchSize);
    }
}
