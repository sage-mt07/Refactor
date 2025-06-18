// =============================================================================
// Messaging Abstractions - Phase 3: 機能分離に対応した統一インターフェース
// =============================================================================

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KsqlDsl.Monitoring.Health;

namespace KsqlDsl.Messaging.Abstractions;

/// <summary>
/// メッセージバス統合調整インターフェース
/// 設計理由：Producer/Consumerの注入構成、統一管理
/// </summary>
public interface IMessageBusCoordinator : IDisposable
{
    /// <summary>
    /// 単一メッセージ送信
    /// </summary>
    Task SendAsync<T>(T message, MessageContext? context = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// バッチメッセージ送信
    /// </summary>
    Task SendBatchAsync<T>(IEnumerable<T> messages, MessageContext? context = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// 購読管理
    /// </summary>
    Task<string> SubscribeAsync<T>(Func<T, MessageContext, Task> handler, SubscriptionOptions? options = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// 購読停止
    /// </summary>
    Task UnsubscribeAsync(string subscriptionId);

    /// <summary>
    /// ヘルス状態取得
    /// </summary>
    Task<MessageBusHealthStatus> GetHealthStatusAsync();

    /// <summary>
    /// 診断情報取得
    /// </summary>
    MessageBusDiagnostics GetDiagnostics();
}

/// <summary>
/// 購読管理インターフェース
/// 設計理由：Consumer系の購読制御責務を分離
/// </summary>
public interface ISubscriptionManager<T> : IDisposable where T : class
{
    /// <summary>
    /// 購読開始
    /// </summary>
    Task<string> StartSubscriptionAsync(Func<T, MessageContext, Task> handler, SubscriptionOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// 購読停止
    /// </summary>
    Task StopSubscriptionAsync(string subscriptionId);

    /// <summary>
    /// 購読一時停止
    /// </summary>
    Task PauseSubscriptionAsync(string subscriptionId);

    /// <summary>
    /// 購読再開
    /// </summary>
    Task ResumeSubscriptionAsync(string subscriptionId);

    /// <summary>
    /// アクティブ購読一覧
    /// </summary>
    List<SubscriptionInfo<T>> GetActiveSubscriptions();

    /// <summary>
    /// 購読統計
    /// </summary>
    SubscriptionStatistics GetStatistics();
}

/// <summary>
/// プール管理共通インターフェース
/// 設計理由：Producer/Consumer両方に適用可能な統一プール管理
/// </summary>
public interface IPoolManager<TKey, T> : IDisposable where TKey : notnull
{
    /// <summary>
    /// リソース取得
    /// </summary>
    T RentResource(TKey key);

    /// <summary>
    /// リソース返却
    /// </summary>
    void ReturnResource(TKey key, T resource);

    /// <summary>
    /// プール統計取得
    /// </summary>
    PoolStatistics GetStatistics();

    /// <summary>
    /// ヘルス状態取得
    /// </summary>
    Task<PoolHealthStatus> GetHealthStatusAsync();

    /// <summary>
    /// リソース数取得
    /// </summary>
    int GetActiveResourceCount();

    /// <summary>
    /// プール最適化実行
    /// </summary>
    void OptimizePools();
}

// =============================================================================
// Data Transfer Objects
// =============================================================================

/// <summary>
/// メッセージコンテキスト（Phase 3統一版）
/// </summary>
public class MessageContext
{
    public string? MessageId { get; set; }
    public string? CorrelationId { get; set; }
    public int? TargetPartition { get; set; }
    public Dictionary<string, object> Headers { get; set; } = new();
    public TimeSpan? Timeout { get; set; }
    public Dictionary<string, object> Tags { get; set; } = new();
    public System.Diagnostics.ActivityContext? ActivityContext { get; set; }
}

/// <summary>
/// 購読オプション
/// </summary>
public class SubscriptionOptions
{
    public string? GroupId { get; set; }
    public bool AutoCommit { get; set; } = true;
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;
    public bool EnablePartitionEof { get; set; } = false;
    public TimeSpan SessionTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(3);
    public bool StopOnError { get; set; } = false;
    public int MaxPollRecords { get; set; } = 500;
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromMinutes(5);
}

/// <summary>
/// 購読情報
/// </summary>
public class SubscriptionInfo<T> where T : class
{
    public string Id { get; set; } = string.Empty;
    public Type EntityType => typeof(T);
    public DateTime StartedAt { get; set; }
    public SubscriptionOptions Options { get; set; } = new();
    public SubscriptionStatus Status { get; set; }
    public long MessagesProcessed { get; set; }
    public DateTime LastMessageAt { get; set; }
}

/// <summary>
/// 購読統計
/// </summary>
public class SubscriptionStatistics
{
    public int ActiveSubscriptions { get; set; }
    public long TotalMessagesProcessed { get; set; }
    public long TotalErrors { get; set; }
    public TimeSpan AverageProcessingTime { get; set; }
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// プール統計
/// </summary>
public class PoolStatistics
{
    public int TotalPools { get; set; }
    public int ActiveResources { get; set; }
    public int PooledResources { get; set; }
    public long TotalRentCount { get; set; }
    public long TotalReturnCount { get; set; }
    public long TotalDiscardedCount { get; set; }
    public double AverageUtilization { get; set; }
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// プールヘルス状態
/// </summary>
//public class PoolHealthStatus
//{
//    public PoolHealthLevel HealthLevel { get; set; }
//    public List<PoolHealthIssue> Issues { get; set; } = new();
//    public PoolStatistics Statistics { get; set; } = new();
//    public DateTime LastCheck { get; set; } = DateTime.UtcNow;
//}

/// <summary>
/// MessageBusヘルス状態
/// </summary>
public class MessageBusHealthStatus
{
    public MessageBusHealthLevel HealthLevel { get; set; }
    public List<MessageBusHealthIssue> Issues { get; set; } = new();
    public DateTime LastCheck { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// MessageBus診断情報
/// </summary>
public class MessageBusDiagnostics
{
    public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;
    public Dictionary<string, object> Configuration { get; set; } = new();
    public PoolStatistics ProducerPoolStats { get; set; } = new();
    public PoolStatistics ConsumerPoolStats { get; set; } = new();
    public SubscriptionStatistics SubscriptionStats { get; set; } = new();
    public Dictionary<string, object> SystemMetrics { get; set; } = new();
}

// =============================================================================
// Enums
// =============================================================================

public enum SubscriptionStatus
{
    Active,
    Paused,
    Stopped,
    Error
}



public enum MessageBusHealthLevel
{
    Healthy,
    Warning,
    Critical
}

// =============================================================================
// Health Issues
// =============================================================================

//public class PoolHealthIssue
//{
//    public PoolHealthIssueType Type { get; set; }
//    public string Description { get; set; } = string.Empty;
//    public PoolIssueSeverity Severity { get; set; }
//    public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
//}

public class MessageBusHealthIssue
{
    public MessageBusHealthIssueType Type { get; set; }
    public string Description { get; set; } = string.Empty;
    public MessageBusIssueSeverity Severity { get; set; }
    public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
}

//public enum PoolHealthIssueType
//{
//    HighFailureRate,
//    PoolExhaustion,
//    ResourceLeak,
//    HealthCheckFailure
//}

public enum MessageBusHealthIssueType
{
    ProducerIssue,
    ConsumerIssue,
    SubscriptionIssue,
    ConfigurationError,
    PerformanceIssue
}

//public enum PoolIssueSeverity
//{
//    Low,
//    Medium,
//    High,
//    Critical
//}

public enum MessageBusIssueSeverity
{
    Low,
    Medium,
    High,
    Critical
}