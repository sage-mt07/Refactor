using KsqlDsl.Core.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace KsqlDsl.Query.Pipeline;

/// <summary>
/// クエリ診断・デバッグ情報管理 - QueryExecutionPipeline用
/// 設計理由：新アーキテクチャでの診断機能提供、既存QueryDiagnosticsを置き換え
/// </summary>
public class QueryDiagnostics
{
    private readonly List<string> _analysisSteps = new();
    private readonly Dictionary<string, object> _metadata = new();
    private readonly ILogger _logger;
    private DateTime _startTime;
    private TimeSpan _analysisTime;

    public Dictionary<string, object> MetaData => _metadata;

    public QueryDiagnostics(ILoggerFactory? loggerFactory = null)
    {
        _startTime = DateTime.UtcNow;
        _logger = loggerFactory.CreateLoggerOrNull<QueryDiagnostics>();
        _logger.LogDebug("QueryDiagnostics initialized at {StartTime}", _startTime);
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

        _logger.LogDebug("Diagnostics step: {Step}", message);
    }

    /// <summary>
    /// メタデータ設定
    /// </summary>
    public void SetMetadata(string key, object value)
    {
        _metadata[key] = value;
        _logger.LogDebug("Diagnostics metadata set: {Key} = {Value}", key, value);
    }

    /// <summary>
    /// 解析完了マーク
    /// </summary>
    public void MarkComplete()
    {
        _analysisTime = DateTime.UtcNow - _startTime;
        var completionMessage = $"Analysis completed in {_analysisTime.TotalMilliseconds:F2}ms";
        LogStep(completionMessage);

        _logger.LogDebug("Query analysis completed in {AnalysisTimeMs:F2}ms", _analysisTime.TotalMilliseconds);
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

        var reportText = report.ToString();
        _logger.LogDebug("Diagnostics report generated with {StepCount} steps and {MetadataCount} metadata entries",
            _analysisSteps.Count, _metadata.Count);

        return reportText;
    }

    /// <summary>
    /// 簡潔サマリ
    /// </summary>
    public string GetSummary()
    {
        var queryType = _metadata.TryGetValue("QueryType", out var qt) ? qt : "Unknown";
        var isPullQuery = _metadata.TryGetValue("IsPullQuery", out var pq) ? pq : false;

        var summary = $"Query: {queryType}, Pull: {isPullQuery}, Time: {_analysisTime.TotalMilliseconds:F1}ms";
        _logger.LogDebug("Diagnostics summary: {Summary}", summary);

        return summary;
    }

    /// <summary>
    /// 診断情報のリセット（再利用時）
    /// </summary>
    public void Reset()
    {
        _analysisSteps.Clear();
        _metadata.Clear();
        _startTime = DateTime.UtcNow;
        _analysisTime = TimeSpan.Zero;

        _logger.LogDebug("QueryDiagnostics reset at {StartTime}", _startTime);
    }
}