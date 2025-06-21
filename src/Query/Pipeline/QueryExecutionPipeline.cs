using KsqlDsl.Core.Extensions;
using KsqlDsl.Query.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace KsqlDsl.Query.Pipeline;

/// <summary>
/// クエリ実行パイプライン - LINQ式から段階的にksqlDBクエリを実行
/// 設計理由：query_redesign.mdの新アーキテクチャに準拠
/// IQueryTranslator互換性：既存インターフェースを実装
/// </summary>
internal class QueryExecutionPipeline : IQueryTranslator, IDisposable
{
    private readonly DerivedObjectManager _derivedObjectManager;
    private readonly DDLQueryGenerator _ddlGenerator;
    private readonly DMLQueryGenerator _dmlGenerator;
    private readonly KsqlDbExecutor _executor;
    private readonly StreamTableAnalyzer _analyzer;
    private readonly ILogger _logger;
    private readonly QueryDiagnostics _diagnostics;
    private bool _disposed = false;

    public QueryExecutionPipeline(
        DerivedObjectManager derivedObjectManager,
        DDLQueryGenerator ddlGenerator,
        DMLQueryGenerator dmlGenerator,
        KsqlDbExecutor executor,
        StreamTableAnalyzer analyzer,
        ILoggerFactory? loggerFactory)
    {
        _derivedObjectManager = derivedObjectManager ?? throw new ArgumentNullException(nameof(derivedObjectManager));
        _ddlGenerator = ddlGenerator ?? throw new ArgumentNullException(nameof(ddlGenerator));
        _dmlGenerator = dmlGenerator ?? throw new ArgumentNullException(nameof(dmlGenerator));
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _analyzer = analyzer ?? throw new ArgumentNullException(nameof(analyzer));
        _logger = loggerFactory.CreateLoggerOrNull<QueryExecutionPipeline>();
        _diagnostics = new QueryDiagnostics(loggerFactory);
    }

    public async Task<QueryExecutionResult> ExecuteQueryAsync<T>(
        string baseObjectName,
        Expression linqExpression,
        QueryExecutionMode mode) where T : class
    {
        _logger.LogDebug("Starting query execution for {BaseObject} with mode {Mode}", baseObjectName, mode);

        try
        {
            // 1. LINQ式を解析
            _logger.LogDebug("Analyzing LINQ expression");
            var analysis = _analyzer.AnalyzeExpression(linqExpression);

            // 2. 必要に応じて派生オブジェクトを作成
            _logger.LogDebug("Creating derived objects if needed");
            var targetObjectName = await CreateDerivedObjectsIfNeeded(baseObjectName, linqExpression, analysis);

            // 3. 最終的なクエリを実行
            _logger.LogDebug("Executing final query on {TargetObject}", targetObjectName);
            var result = await ExecuteFinalQuery<T>(targetObjectName, mode);

            _logger.LogInformation("Query execution completed successfully for {BaseObject}", baseObjectName);

            return new QueryExecutionResult
            {
                Success = true,
                TargetObject = targetObjectName,
                Data = result,
                ExecutedAt = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Query execution failed for {BaseObject}", baseObjectName);
            throw;
        }
    }

    private async Task<string> CreateDerivedObjectsIfNeeded(
        string baseObjectName,
        Expression linqExpression,
        ExpressionAnalysisResult analysis)
    {
        var currentObjectName = baseObjectName;

        _logger.LogDebug("Processing {MethodCallCount} method calls for derived object creation", analysis.MethodCalls.Count);

        foreach (var methodCall in analysis.MethodCalls)
        {
            var methodName = methodCall.Method.Name;
            _logger.LogDebug("Processing method call: {MethodName} on {CurrentObject}", methodName, currentObjectName);

            switch (methodName)
            {
                case "Where":
                    currentObjectName = await _derivedObjectManager.CreateDerivedStreamAsync(
                        currentObjectName, methodCall);
                    _logger.LogDebug("Created derived stream for WHERE: {DerivedObject}", currentObjectName);
                    break;

                case "Select":
                    if (analysis.HasAggregation)
                    {
                        currentObjectName = await _derivedObjectManager.CreateDerivedTableAsync(
                            currentObjectName, methodCall);
                        _logger.LogDebug("Created derived table for SELECT (aggregation): {DerivedObject}", currentObjectName);
                    }
                    else
                    {
                        currentObjectName = await _derivedObjectManager.CreateDerivedStreamAsync(
                            currentObjectName, methodCall);
                        _logger.LogDebug("Created derived stream for SELECT: {DerivedObject}", currentObjectName);
                    }
                    break;

                case "GroupBy":
                    currentObjectName = await _derivedObjectManager.CreateDerivedTableAsync(
                        currentObjectName, methodCall);
                    _logger.LogDebug("Created derived table for GROUP BY: {DerivedObject}", currentObjectName);
                    break;
            }
        }

        return currentObjectName;
    }

    private async Task<List<T>> ExecuteFinalQuery<T>(string objectName, QueryExecutionMode mode) where T : class
    {
        // 最終クエリは常にSELECT * FROM のみ
        // すべての条件は派生オブジェクト作成時に適用済み
        var query = _dmlGenerator.GenerateSelectAll(objectName, mode == QueryExecutionMode.PullQuery);

        _logger.LogDebug("Executing final query: {Query}", query);

        return mode switch
        {
            QueryExecutionMode.PullQuery => await _executor.ExecutePullQueryAsync<T>(query),
            QueryExecutionMode.PushQuery => await _executor.ExecutePushQueryAsync<T>(query),
            _ => throw new ArgumentException($"Unsupported execution mode: {mode}")
        };
    }

    // IQueryTranslator実装（既存インターフェース互換性）
    public string ToKsql(Expression expression, string topicName, bool isPullQuery = false)
    {
        _logger.LogDebug("IQueryTranslator.ToKsql called for {TopicName}", topicName);
        _diagnostics.LogStep("IQueryTranslator.ToKsql called", new { TopicName = topicName, IsPullQuery = isPullQuery });

        return GenerateKsqlQuery(topicName, expression, isPullQuery);
    }

    public string GetDiagnostics()
    {
        return _diagnostics.GenerateReport();
    }

    public bool IsPullQuery()
    {
        return _diagnostics.MetaData.TryGetValue("IsPullQuery", out var value) && (bool)value;
    }

    public string GenerateKsqlQuery(string baseObjectName, Expression linqExpression, bool isPullQuery)
    {
        _logger.LogDebug("Generating KSQL query for {BaseObject}, isPullQuery: {IsPullQuery}", baseObjectName, isPullQuery);

        try
        {
            // 実際のクエリ実行パスと同じロジックでKSQL生成
            var analysis = _analyzer.AnalyzeExpression(linqExpression);

            // 派生オブジェクト名をシミュレート
            var targetObjectName = SimulateDerivedObjectCreation(baseObjectName, analysis);

            // 最終クエリ生成
            var query = _dmlGenerator.GenerateSelectAll(targetObjectName, isPullQuery);

            _logger.LogDebug("Generated KSQL query: {Query}", query);
            return query;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to generate KSQL query for {BaseObject}", baseObjectName);
            return $"/* KSQL生成エラー: {ex.Message} */";
        }
    }

    private string SimulateDerivedObjectCreation(string baseObjectName, ExpressionAnalysisResult analysis)
    {
        var currentObjectName = baseObjectName;

        foreach (var methodCall in analysis.MethodCalls)
        {
            switch (methodCall.Method.Name)
            {
                case "Where":
                case "Select":
                case "GroupBy":
                    // シミュレーション用の名前生成
                    var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    var objectType = (methodCall.Method.Name == "GroupBy" || analysis.HasAggregation) ? "table" : "stream";
                    currentObjectName = $"{currentObjectName}_{objectType}_{timestamp}";
                    break;
            }
        }

        return currentObjectName;
    }

    public async Task StopAllStreamingQueriesAsync()
    {
        _logger.LogInformation("Stopping all streaming queries");

        try
        {
            // ストリーミングクエリの停止処理
            // 実装は KsqlDbExecutor に依存
            await _executor.StopAllQueriesAsync();
            _logger.LogInformation("All streaming queries stopped successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to stop streaming queries");
            throw;
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _logger.LogDebug("Disposing QueryExecutionPipeline");

            try
            {
                // 派生オブジェクトのクリーンアップ
                _derivedObjectManager?.CleanupDerivedObjects();

                // エグゼキューターの停止
                _executor?.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during QueryExecutionPipeline disposal");
            }

            _disposed = true;
            _logger.LogDebug("QueryExecutionPipeline disposed");
        }
    }
}
