using Confluent.Kafka;
using KsqlDsl.Configuration.Abstractions;

using System;
using System.Collections.Generic;

namespace KsqlDsl.Messaging.Configuration;


/// <summary>
/// Consumer設定
/// </summary>
public class KafkaConsumerConfig
{
    // 基本設定
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string DefaultGroupId { get; set; } = "ksqldsl-default";
    public Confluent.Kafka.AutoOffsetReset AutoOffsetReset { get; set; } = Confluent.Kafka.AutoOffsetReset.Latest;
    public bool EnableAutoCommit { get; set; } = true;
    public int AutoCommitIntervalMs { get; set; } = 5000;

    // セッション設定
    public int SessionTimeoutMs { get; set; } = 30000;
    public int HeartbeatIntervalMs { get; set; } = 3000;
    public int MaxPollIntervalMs { get; set; } = 300000;
    public int MaxPollRecords { get; set; } = 500;

    // フェッチ設定
    public int FetchMinBytes { get; set; } = 1;
    public int FetchMaxWaitMs { get; set; } = 500;
    public int FetchMaxBytes { get; set; } = 52428800; // 50MB

    // セキュリティ設定
    public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.Plaintext;
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }

    // 追加設定
    public Dictionary<string, object> AdditionalConfig { get; set; } = new();

    // ヘルス閾値
    public ConsumerHealthThresholds HealthThresholds { get; set; } = new();

    public int GetKeyHash()
    {
        return HashCode.Combine(
            BootstrapServers, DefaultGroupId, AutoOffsetReset, EnableAutoCommit,
            SessionTimeoutMs, MaxPollRecords);
    }
}
