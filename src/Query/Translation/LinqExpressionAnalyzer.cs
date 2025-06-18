using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Translation
{
    /// <summary>
    /// LINQ式木の構文解析
    /// 設計理由：LinqToKsqlTranslatorから式木解析責務を分離
    /// </summary>
    public class LinqExpressionAnalyzer : ExpressionVisitor
    {
        private readonly List<MethodCallExpression> _methodCalls = new();
        private readonly HashSet<string> _aggregateMethods = new();
        private bool _hasGroupBy = false;
        private bool _hasJoin = false;
        private bool _hasWindow = false;

        public LinqExpressionAnalyzer()
        {
            InitializeAggregateMethods();
        }

        /// <summary>
        /// 式木を解析してメタデータを抽出
        /// </summary>
        public ExpressionAnalysisResult Analyze(Expression expression)
        {
            // 状態リセット
            _methodCalls.Clear();
            _hasGroupBy = _hasJoin = _hasWindow = false;

            // 式木訪問
            Visit(expression);

            return new ExpressionAnalysisResult
            {
                MethodCalls = _methodCalls.ToList(),
                HasGroupBy = _hasGroupBy,
                HasJoin = _hasJoin,
                HasWindow = _hasWindow,
                HasAggregation = _methodCalls.Any(mc => _aggregateMethods.Contains(mc.Method.Name)),
                QueryType = DetermineQueryType()
            };
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            _methodCalls.Add(node);

            switch (node.Method.Name)
            {
                case "GroupBy":
                    _hasGroupBy = true;
                    break;
                case "Join":
                    _hasJoin = true;
                    break;
                case "Window":
                case "TumblingWindow":
                case "HoppingWindow":
                case "SessionWindow":
                    _hasWindow = true;
                    break;
            }

            return base.VisitMethodCall(node);
        }

        private void InitializeAggregateMethods()
        {
            var methods = new[] { "Sum", "Count", "Max", "Min", "Average", "Avg",
                                "LatestByOffset", "EarliestByOffset", "CollectList", "CollectSet" };
            foreach (var method in methods)
            {
                _aggregateMethods.Add(method);
            }
        }

        private QueryType DetermineQueryType()
        {
            if (_hasJoin) return QueryType.Join;
            if (_hasGroupBy || _aggregateMethods.Any(m => _methodCalls.Any(mc => mc.Method.Name == m)))
                return QueryType.Aggregate;
            if (_hasWindow) return QueryType.Windowed;

            return QueryType.Simple;
        }
    }

    /// <summary>
    /// 式木解析結果
    /// </summary>
    public class ExpressionAnalysisResult
    {
        public List<MethodCallExpression> MethodCalls { get; set; } = new();
        public bool HasGroupBy { get; set; }
        public bool HasJoin { get; set; }
        public bool HasWindow { get; set; }
        public bool HasAggregation { get; set; }
        public QueryType QueryType { get; set; }
    }

    public enum QueryType
    {
        Simple,
        Aggregate,
        Join,
        Windowed
    }
}