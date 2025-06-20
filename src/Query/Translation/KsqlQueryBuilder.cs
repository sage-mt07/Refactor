using KsqlDsl.Query.Abstractions;
using KsqlDsl.Query.Builders;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace KsqlDsl.Query.Translation
{
    /// <summary>
    /// KSQL構文構築ロジック
    /// 設計理由：LinqToKsqlTranslatorから構文構築責務を分離
    /// </summary>
    public class KsqlQueryBuilder
    {
        private readonly IKsqlBuilder _selectBuilder;
        private readonly IKsqlBuilder _joinBuilder;
        private readonly IKsqlBuilder _groupByBuilder;
        private readonly IKsqlBuilder _havingBuilder;
        private readonly IKsqlBuilder _windowBuilder;
        private readonly IKsqlBuilder _projectionBuilder;

        public KsqlQueryBuilder()
        {
            _selectBuilder = new SelectBuilder();
            _joinBuilder = new JoinBuilder();
            _groupByBuilder = new GroupByBuilder();
            _havingBuilder = new HavingBuilder();
            _windowBuilder = new WindowBuilder();
            _projectionBuilder = new ProjectionBuilder();
        }

        /// <summary>
        /// 解析結果からKSQL文を構築
        /// </summary>
        public string BuildQuery(ExpressionAnalysisResult analysisResult, string topicName, bool isPullQuery)
        {
            var query = new StringBuilder();

            // SELECT句構築
            BuildSelectClause(query, analysisResult);

            // FROM句
            query.Append($" FROM {topicName}");

            // WHERE句、GROUP BY句等の構築
            BuildQueryClauses(query, analysisResult);

            // EMIT句制御
            if (!isPullQuery)
            {
                query.Append(" EMIT CHANGES");
            }

            return query.ToString();
        }

        private void BuildSelectClause(StringBuilder query, ExpressionAnalysisResult result)
        {
            var selectCall = result.MethodCalls.LastOrDefault(mc => mc.Method.Name == "Select");

            if (selectCall != null)
            {
                if (result.HasAggregation)
                {
                    // 集約クエリの場合は特別処理
                    query.Append(BuildAggregateSelect(selectCall));
                }
                else
                {
                    query.Append(_projectionBuilder.Build(selectCall));
                }
            }
            else
            {
                query.Append("SELECT *");
            }
        }

        private void BuildQueryClauses(StringBuilder query, ExpressionAnalysisResult result)
        {
            // WHERE句
            var whereCall = result.MethodCalls.FirstOrDefault(mc => mc.Method.Name == "Where");
            if (whereCall != null)
            {
                // 実装詳細は既存KsqlConditionBuilderを活用
                query.Append(" ").Append(BuildWhereClause(whereCall));
            }

            // GROUP BY句
            if (result.HasGroupBy)
            {
                var groupByCall = result.MethodCalls.First(mc => mc.Method.Name == "GroupBy");
                query.Append(" ").Append(_groupByBuilder.Build(groupByCall));
            }

            // JOIN句処理
            if (result.HasJoin)
            {
                var joinCall = result.MethodCalls.First(mc => mc.Method.Name == "Join");
                // JOINの場合は構文全体を再構築
                _joinBuilder.Build(joinCall);
            }
        }

        private string BuildAggregateSelect(Expression selectExpression)
        {
            // 既存KsqlAggregateBuilderを活用
            return _groupByBuilder.Build(selectExpression);
        }

        private string BuildWhereClause(Expression whereExpression)
        {
            return _selectBuilder.Build(whereExpression);
        }
    }
}
