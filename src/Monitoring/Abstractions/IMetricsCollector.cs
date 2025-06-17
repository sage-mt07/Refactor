using System;
using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// メトリクス収集機能の統一インターフェース
    /// 設計理由：メトリクス収集の責務統一、型安全性確保
    /// </summary>
    /// <typeparam name="T">メトリクス対象の型</typeparam>
    public interface IMetricsCollector<T>
    {
        /// <summary>
        /// カウンターメトリクスの記録
        /// </summary>
        /// <param name="name">メトリクス名</param>
        /// <param name="value">カウント値</param>
        /// <param name="tags">タグ情報</param>
        void RecordCounter(string name, long value, IDictionary<string, object?>? tags = null);

        /// <summary>
        /// ゲージメトリクスの記録
        /// </summary>
        /// <param name="name">メトリクス名</param>
        /// <param name="value">ゲージ値</param>
        /// <param name="tags">タグ情報</param>
        void RecordGauge(string name, double value, IDictionary<string, object?>? tags = null);

        /// <summary>
        /// ヒストグラムメトリクスの記録
        /// </summary>
        /// <param name="name">メトリクス名</param>
        /// <param name="value">測定値</param>
        /// <param name="tags">タグ情報</param>
        void RecordHistogram(string name, double value, IDictionary<string, object?>? tags = null);

        /// <summary>
        /// 時間計測メトリクスの記録
        /// </summary>
        /// <param name="name">メトリクス名</param>
        /// <param name="duration">実行時間</param>
        /// <param name="tags">タグ情報</param>
        void RecordTimer(string name, TimeSpan duration, IDictionary<string, object?>? tags = null);

        /// <summary>
        /// 収集されたメトリクスの取得
        /// </summary>
        /// <returns>メトリクス統計情報</returns>
        MetricsSnapshot GetSnapshot();

        /// <summary>
        /// メトリクス収集対象の型名
        /// </summary>
        string TargetTypeName { get; }

        /// <summary>
        /// 収集開始時刻
        /// </summary>
        DateTime StartTime { get; }
    }

    /// <summary>
    /// メトリクススナップショット
    /// </summary>
    public class MetricsSnapshot
    {
        public string TargetTypeName { get; set; } = string.Empty;
        public DateTime SnapshotTime { get; set; } = DateTime.UtcNow;
        public Dictionary<string, CounterMetric> Counters { get; set; } = new();
        public Dictionary<string, GaugeMetric> Gauges { get; set; } = new();
        public Dictionary<string, HistogramMetric> Histograms { get; set; } = new();
        public Dictionary<string, TimerMetric> Timers { get; set; } = new();
    }

    /// <summary>
    /// カウンターメトリクス
    /// </summary>
    public class CounterMetric
    {
        public string Name { get; set; } = string.Empty;
        public long Value { get; set; }
        public DateTime LastUpdated { get; set; }
        public IDictionary<string, object?> Tags { get; set; } = new Dictionary<string, object?>();
    }

    /// <summary>
    /// ゲージメトリクス
    /// </summary>
    public class GaugeMetric
    {
        public string Name { get; set; } = string.Empty;
        public double Value { get; set; }
        public DateTime LastUpdated { get; set; }
        public IDictionary<string, object?> Tags { get; set; } = new Dictionary<string, object?>();
    }

    /// <summary>
    /// ヒストグラムメトリクス
    /// </summary>
    public class HistogramMetric
    {
        public string Name { get; set; } = string.Empty;
        public long Count { get; set; }
        public double Sum { get; set; }
        public double Min { get; set; }
        public double Max { get; set; }
        public double Average => Count > 0 ? Sum / Count : 0;
        public DateTime LastUpdated { get; set; }
        public IDictionary<string, object?> Tags { get; set; } = new Dictionary<string, object?>();
    }

    /// <summary>
    /// タイマーメトリクス
    /// </summary>
    public class TimerMetric
    {
        public string Name { get; set; } = string.Empty;
        public long Count { get; set; }
        public TimeSpan TotalTime { get; set; }
        public TimeSpan MinTime { get; set; }
        public TimeSpan MaxTime { get; set; }
        public TimeSpan AverageTime => Count > 0 ? TimeSpan.FromTicks(TotalTime.Ticks / Count) : TimeSpan.Zero;
        public DateTime LastUpdated { get; set; }
        public IDictionary<string, object?> Tags { get; set; } = new Dictionary<string, object?>();
    }
}