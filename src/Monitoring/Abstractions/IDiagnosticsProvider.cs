using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// 診断情報提供機能の統一インターフェース
    /// 設計理由：診断情報の責務統一、トラブルシューティング支援
    /// </summary>
    public interface IDiagnosticsProvider
    {
        /// <summary>
        /// 診断情報の収集
        /// </summary>
        /// <param name="cancellationToken">キャンセルトークン</param>
        /// <returns>診断情報</returns>
        Task<DiagnosticsInfo> GetDiagnosticsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// 診断対象コンポーネント名
        /// </summary>
        string ComponentName { get; }

        /// <summary>
        /// 診断情報のカテゴリ
        /// </summary>
        DiagnosticsCategory Category { get; }

        /// <summary>
        /// 診断の重要度
        /// </summary>
        DiagnosticsPriority Priority { get; }
    }

    /// <summary>
    /// 診断情報
    /// </summary>
    public class DiagnosticsInfo
    {
        public string ComponentName { get; set; } = string.Empty;
        public DiagnosticsCategory Category { get; set; }
        public DiagnosticsPriority Priority { get; set; }
        public DateTime CollectedAt { get; set; } = DateTime.UtcNow;
        public TimeSpan CollectionDuration { get; set; }

        public Dictionary<string, object> Properties { get; set; } = new();
        public List<DiagnosticsEntry> Entries { get; set; } = new();
        public List<DiagnosticsWarning> Warnings { get; set; } = new();
        public List<DiagnosticsError> Errors { get; set; } = new();
    }

    /// <summary>
    /// 診断エントリ
    /// </summary>
    public class DiagnosticsEntry
    {
        public string Key { get; set; } = string.Empty;
        public object? Value { get; set; }
        public string? Description { get; set; }
        public DiagnosticsEntryType Type { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }

    /// <summary>
    /// 診断警告
    /// </summary>
    public class DiagnosticsWarning
    {
        public string Message { get; set; } = string.Empty;
        public string? Code { get; set; }
        public object? Context { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }

    /// <summary>
    /// 診断エラー
    /// </summary>
    public class DiagnosticsError
    {
        public string Message { get; set; } = string.Empty;
        public string? Code { get; set; }
        public Exception? Exception { get; set; }
        public object? Context { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }

    /// <summary>
    /// 診断カテゴリ
    /// </summary>
    public enum DiagnosticsCategory
    {
        Performance,
        Configuration,
        Connectivity,
        Schema,
        Cache,
        Pool,
        Serialization,
        General
    }

    /// <summary>
    /// 診断の重要度
    /// </summary>
    public enum DiagnosticsPriority
    {
        Low,
        Normal,
        High,
        Critical
    }

    /// <summary>
    /// 診断エントリの種別
    /// </summary>
    public enum DiagnosticsEntryType
    {
        Counter,
        Gauge,
        Configuration,
        Status,
        Timestamp,
        Error,
        Warning,
        Info
    }
}