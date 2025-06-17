using System;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions
{
    /// <summary>
    /// ヘルス監視インターフェース
    /// Monitoring層との依存逆転
    /// </summary>
    public interface IHealthMonitor
    {
        Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default);
        string ComponentName { get; }
        HealthLevel Level { get; }
        event EventHandler<HealthStateChangedEventArgs>? HealthStateChanged;
    }

    public class HealthCheckResult
    {
        public HealthStatus Status { get; set; }
        public string Description { get; set; } = string.Empty;
        public TimeSpan Duration { get; set; }
        public Exception? Exception { get; set; }
        public object? Data { get; set; }
        public DateTime CheckedAt { get; set; } = DateTime.UtcNow;
    }

    public enum HealthStatus
    {
        Healthy,
        Degraded,
        Unhealthy,
        Unknown
    }

    public enum HealthLevel
    {
        Info,
        Warning,
        Critical
    }

    public class HealthStateChangedEventArgs : EventArgs
    {
        public HealthStatus PreviousStatus { get; set; }
        public HealthStatus CurrentStatus { get; set; }
        public string ComponentName { get; set; } = string.Empty;
        public DateTime ChangedAt { get; set; } = DateTime.UtcNow;
        public string? Reason { get; set; }
    }
}