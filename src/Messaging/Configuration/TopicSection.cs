using System.Collections.Generic;

namespace KsqlDsl.Messaging.Configuration;

/// <summary>
/// トピック別設定（Producer/Consumer両方の設定を含む）
/// </summary>
public class TopicSection
{
    /// <summary>
    /// トピック名（オプション - Dictionaryのキーからも取得可能）
    /// </summary>
    public string? TopicName { get; init; }

    /// <summary>
    /// Producer設定
    /// </summary>
    public ProducerSection Producer { get; init; } = new();

    /// <summary>
    /// Consumer設定
    /// </summary>
    public ConsumerSection Consumer { get; init; } = new();

    /// <summary>
    /// トピック作成時の設定（パーティション数、レプリケーション係数等）
    /// </summary>
    public TopicCreationSection? Creation { get; init; }
}

/// <summary>
/// トピック作成設定
/// </summary>
public class TopicCreationSection
{
    /// <summary>
    /// パーティション数
    /// </summary>
    public int NumPartitions { get; init; } = 1;

    /// <summary>
    /// レプリケーション係数
    /// </summary>
    public short ReplicationFactor { get; init; } = 1;

    /// <summary>
    /// トピック設定（cleanup.policy, retention.ms等）
    /// </summary>
    public Dictionary<string, string> Configs { get; init; } = new();

    /// <summary>
    /// 自動作成を有効にするか
    /// </summary>
    public bool EnableAutoCreation { get; init; } = false;
}