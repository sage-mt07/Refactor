using System.Collections.Generic;

namespace KsqlDsl.Messaging.Configuration;

/// <summary>
/// Consumer設定
/// </summary>
public class ConsumerSection
{
    /// <summary>
    /// コンシューマーグループID
    /// </summary>
    public string GroupId { get; init; } = string.Empty;

    /// <summary>
    /// オートオフセットリセット（Latest, Earliest, None）
    /// </summary>
    public string AutoOffsetReset { get; init; } = "Latest";

    /// <summary>
    /// 自動コミット有効化
    /// </summary>
    public bool EnableAutoCommit { get; init; } = true;

    /// <summary>
    /// 自動コミット間隔（ミリ秒）
    /// </summary>
    public int AutoCommitIntervalMs { get; init; } = 5000;

    /// <summary>
    /// セッションタイムアウト（ミリ秒）
    /// </summary>
    public int SessionTimeoutMs { get; init; } = 30000;

    /// <summary>
    /// ハートビート間隔（ミリ秒）
    /// </summary>
    public int HeartbeatIntervalMs { get; init; } = 3000;

    /// <summary>
    /// 最大ポーリング間隔（ミリ秒）
    /// </summary>
    public int MaxPollIntervalMs { get; init; } = 300000;

    /// <summary>
    /// 最大ポーリングレコード数
    /// </summary>
    public int MaxPollRecords { get; init; } = 500;

    /// <summary>
    /// フェッチ最小バイト数
    /// </summary>
    public int FetchMinBytes { get; init; } = 1;

    /// <summary>
    /// フェッチ最大待機時間（ミリ秒）
    /// </summary>
    public int FetchMaxWaitMs { get; init; } = 500;

    /// <summary>
    /// フェッチ最大バイト数
    /// </summary>
    public int FetchMaxBytes { get; init; } = 52428800; // 50MB

    /// <summary>
    /// パーティション割り当て戦略
    /// </summary>
    public string? PartitionAssignmentStrategy { get; init; }

    /// <summary>
    /// アイソレーションレベル
    /// </summary>
    public string IsolationLevel { get; init; } = "ReadUncommitted";

    /// <summary>
    /// 追加設定プロパティ（運用上の柔軟性確保）
    /// </summary>
    public Dictionary<string, string> AdditionalProperties { get; init; } = new();
}