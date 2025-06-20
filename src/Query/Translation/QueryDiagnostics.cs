using System;
using System.Collections.Generic;
using System.Text;

namespace KsqlDsl.Query.Translation
{
    /// <summary>
    /// クエリ診断・デバッグ情報管理
    /// 設計理由：デバッグ出力・生成ログの責務分離
    /// </summary>
    public class QueryDiagnostics
    {
        private readonly List<string> _analysisSteps = new();
        private readonly Dictionary<string, object> _metadata = new();
        private DateTime _startTime;
        private TimeSpan _analysisTime;

        public Dictionary<string, object> MetaData => _metadata;
        public QueryDiagnostics()
        {
            _startTime = DateTime.UtcNow;
        }

        /// <summary>
        /// 解析ステップを記録
        /// </summary>
        public void LogStep(string step, object? detail = null)
        {
            var timestamp = DateTime.UtcNow.ToString("HH:mm:ss.fff");
            var message = $"[{timestamp}] {step}";

            if (detail != null)
            {
                message += $": {detail}";
            }

            _analysisSteps.Add(message);
        }

        /// <summary>
        /// メタデータ設定
        /// </summary>
        public void SetMetadata(string key, object value)
        {
            _metadata[key] = value;
        }

        /// <summary>
        /// 解析完了マーク
        /// </summary>
        public void MarkComplete()
        {
            _analysisTime = DateTime.UtcNow - _startTime;
            LogStep($"Analysis completed in {_analysisTime.TotalMilliseconds:F2}ms");
        }

        /// <summary>
        /// 診断レポート生成
        /// </summary>
        public string GenerateReport()
        {
            var report = new StringBuilder();

            report.AppendLine("=== KSQL Query Diagnostics ===");
            report.AppendLine($"Analysis Duration: {_analysisTime.TotalMilliseconds:F2}ms");
            report.AppendLine();

            // メタデータ出力
            if (_metadata.Count > 0)
            {
                report.AppendLine("Metadata:");
                foreach (var kvp in _metadata)
                {
                    report.AppendLine($"  {kvp.Key}: {kvp.Value}");
                }
                report.AppendLine();
            }

            // ステップ履歴
            report.AppendLine("Analysis Steps:");
            foreach (var step in _analysisSteps)
            {
                report.AppendLine($"  {step}");
            }

            return report.ToString();
        }

        /// <summary>
        /// 簡潔サマリ
        /// </summary>
        public string GetSummary()
        {
            var queryType = _metadata.TryGetValue("QueryType", out var qt) ? qt : "Unknown";
            var isPullQuery = _metadata.TryGetValue("IsPullQuery", out var pq) ? pq : false;

            return $"Query: {queryType}, Pull: {isPullQuery}, Time: {_analysisTime.TotalMilliseconds:F1}ms";
        }
    }
}