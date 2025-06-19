using KsqlDsl.Query.Abstractions;
using System;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Translation
{
    /// <summary>
    /// QueryTranslatorの実装クラス
    /// 設計理由：既存LinqToKsqlTranslatorをIQueryTranslator準拠で再構築
    /// </summary>
    public class QueryTranslator : IQueryTranslator
    {
        private readonly LinqExpressionAnalyzer _analyzer;
        private readonly KsqlQueryBuilder _queryBuilder;
        private readonly QueryDiagnostics _diagnostics;

        public QueryTranslator()
        {
            _analyzer = new LinqExpressionAnalyzer();
            _queryBuilder = new KsqlQueryBuilder();
            _diagnostics = new QueryDiagnostics();
        }

        public string ToKsql(Expression expression, string topicName, bool isPullQuery = false)
        {
            _diagnostics.LogStep("Translation started", new { TopicName = topicName, IsPullQuery = isPullQuery });
            _diagnostics.SetMetadata("TopicName", topicName);
            _diagnostics.SetMetadata("IsPullQuery", isPullQuery);

            try
            {
                // 式木解析
                _diagnostics.LogStep("Analyzing LINQ expression");
                var analysisResult = _analyzer.Analyze(expression);
                _diagnostics.SetMetadata("QueryType", analysisResult.QueryType);
                _diagnostics.LogStep("Analysis completed", analysisResult.QueryType);

                // KSQL構築
                _diagnostics.LogStep("Building KSQL query");
                var ksqlQuery = _queryBuilder.BuildQuery(analysisResult, topicName, isPullQuery);
                _diagnostics.LogStep("KSQL built successfully", ksqlQuery.Length + " characters");

                _diagnostics.MarkComplete();
                return ksqlQuery;
            }
            catch (Exception ex)
            {
                _diagnostics.LogStep("Translation failed", ex.Message);
                _diagnostics.MarkComplete();
                throw;
            }
        }

        public string GetDiagnostics()
        {
            return _diagnostics.GenerateReport();
        }

        public bool IsPullQuery()
        {
            return _diagnostics.MetaData.TryGetValue("IsPullQuery", out var value) && (bool)value;
        }
    }
}