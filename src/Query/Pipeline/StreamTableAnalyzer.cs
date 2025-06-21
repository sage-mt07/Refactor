using KsqlDsl.Core.Extensions;
using KsqlDsl.Query.Abstractions;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Pipeline;

/// <summary>
/// Stream/Table型推論アナライザー
/// 設計理由：query_redesign.mdの新アーキテクチャに準拠
/// </summary>
internal class StreamTableAnalyzer
{
    private readonly Dictionary<string, OperationCharacteristics> _operationMap;
    private readonly ILogger _logger;

    public StreamTableAnalyzer(ILoggerFactory? loggerFactory = null)
    {
        _logger = loggerFactory.CreateLoggerOrNull<StreamTableAnalyzer>();

        _operationMap = new Dictionary<string, OperationCharacteristics>
        {
            ["Where"] = new OperationCharacteristics
            {
                ResultType = StreamTableType.Stream,
                RequiresDerivedObject = true,
                CanChain = true
            },
            ["Select"] = new OperationCharacteristics
            {
                ResultType =StreamTableType.Stream,
                RequiresDerivedObject = true,
                CanChain = true,
                DependsOnAggregation = true
            },
            ["GroupBy"] = new OperationCharacteristics
            {
                ResultType =StreamTableType.Table,
                RequiresDerivedObject = true,
                CanChain = false,
                IsAggregation = true
            },
            ["Take"] = new OperationCharacteristics
            {
                ResultType = StreamTableType.Stream,
                RequiresDerivedObject = false,
                CanChain = false
            },
            ["Skip"] = new OperationCharacteristics
            {
                ResultType =StreamTableType.Stream,
                RequiresDerivedObject = false,
                CanChain = false
            }
        };

        _logger.LogDebug("StreamTableAnalyzer initialized with {OperationCount} operation mappings", _operationMap.Count);
    }

    public ExpressionAnalysisResult AnalyzeExpression(Expression expression)
    {
        _logger.LogDebug("Starting expression analysis");

        var visitor = new ExpressionAnalysisVisitor(_operationMap, _logger);
        visitor.Visit(expression);

        var result = new ExpressionAnalysisResult
        {
            MethodCalls = visitor.MethodCalls,
            HasAggregation = visitor.HasAggregation,
            HasGroupBy = visitor.HasGroupBy,
            RequiresStreamOutput = visitor.RequiresStreamOutput,
            RequiresTableOutput = visitor.RequiresTableOutput,
            OperationChain = visitor.OperationChain
        };

        _logger.LogDebug("Expression analysis completed. HasAggregation: {HasAggregation}, HasGroupBy: {HasGroupBy}, " +
            "RequiresStreamOutput: {RequiresStreamOutput}, RequiresTableOutput: {RequiresTableOutput}",
            result.HasAggregation, result.HasGroupBy, result.RequiresStreamOutput, result.RequiresTableOutput);

        return result;
    }

    private class ExpressionAnalysisVisitor : ExpressionVisitor
    {
        private readonly Dictionary<string, OperationCharacteristics> _operationMap;
        private readonly ILogger _logger;

        public List<MethodCallExpression> MethodCalls { get; } = new();
        public List<OperationInfo> OperationChain { get; } = new();
        public bool HasAggregation { get; private set; }
        public bool HasGroupBy { get; private set; }
        public bool RequiresStreamOutput { get; private set; } = true;
        public bool RequiresTableOutput { get; private set; }

        public ExpressionAnalysisVisitor(Dictionary<string, OperationCharacteristics> operationMap, ILogger logger)
        {
            _operationMap = operationMap;
            _logger = logger;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var methodName = node.Method.Name;
            MethodCalls.Add(node);

            _logger.LogDebug("Analyzing method call: {MethodName}", methodName);

            if (_operationMap.TryGetValue(methodName, out var characteristics))
            {
                var operationInfo = new OperationInfo
                {
                    MethodName = methodName,
                    Expression = node,
                    Characteristics = characteristics,
                    Order = OperationChain.Count
                };

                OperationChain.Add(operationInfo);

                // Update analysis state
                if (characteristics.IsAggregation)
                {
                    HasAggregation = true;
                    _logger.LogDebug("Detected aggregation operation: {MethodName}", methodName);
                }

                if (methodName == "GroupBy")
                {
                    HasGroupBy = true;
                    RequiresTableOutput = true;
                    RequiresStreamOutput = false;
                    _logger.LogDebug("Detected GroupBy operation - requires TABLE output");
                }

                // Select after GroupBy should produce Table
                if (methodName == "Select" && HasGroupBy)
                {
                    RequiresTableOutput = true;
                    RequiresStreamOutput = false;
                    _logger.LogDebug("Select after GroupBy - requires TABLE output");
                }
            }

            return base.VisitMethodCall(node);
        }
    }
}