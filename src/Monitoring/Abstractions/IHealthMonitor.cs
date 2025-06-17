using System;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// ヘルス監視機能の統一インターフェース
    /// 設計理由：監視系機能の責務統一、プラグイン化による拡張性確保
    /// </summary>
    public interface IHealthMonitor
    {
        /// <summary>
        /// ヘルスチェック実行
        /// </summary>
        /// <param name="cancellationToken">キャンセルトークン</param>
        /// <returns>ヘルスチェック結果</returns>
        Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// 監視対象コンポーネント名
        /// </summary>
        string ComponentName { get; }

        /// <summary>
        /// 監視レベル（Critical/Warning/Info）
        /// </summary>
        HealthLevel Level { get; }

        /// <summary>
        /// ヘルス状態変更時の通知
        /// </summary>
        event EventHandler<HealthStateChangedEventArgs>? HealthStateChanged;
    }

    /// <summary>
    /// ヘルスチェック結果
    /// </summary>
    public class HealthCheckResult
    {
        public HealthStatus Status { get; set; }
        public string Description { get; set; } = string.Empty;
        public TimeSpan Duration { get; set; }
        public Exception? Exception { get; set; }
        public object? Data { get; set; }
        public DateTime CheckedAt { get; set; } = DateTime.UtcNow;
    }

    /// <summary>
    /// ヘルス状態
    /// </summary>
    public enum HealthStatus
    {
        Healthy,
        Degraded,
        Unhealthy,
        Unknown
    }

    /// <summary>
    /// 監視レベル
    /// </summary>
    public enum HealthLevel
    {
        Info,
        Warning,
        Critical
    }

    /// <summary>
    /// ヘルス状態変更イベント引数
    /// </summary>
    public class HealthStateChangedEventArgs : EventArgs
    {
        public HealthStatus PreviousStatus { get; set; }
        public HealthStatus CurrentStatus { get; set; }
        public string ComponentName { get; set; } = string.Empty;
        public DateTime ChangedAt { get; set; } = DateTime.UtcNow;
        public string? Reason { get; set; }
    }
}