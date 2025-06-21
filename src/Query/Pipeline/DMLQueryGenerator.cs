using KsqlDsl.Core.Extensions;
using KsqlDsl.Query.Abstractions;
using KsqlDsl.Query.Builders;
using Microsoft.Extensions.Logging;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Pipeline
{
    /// <summary>
    /// DMLクエリ生成器 - SELECT文の生成
    /// 設計理由：query_redesign.mdの新アーキテクチャに準拠
    /// IKsqlBuilder互換性：既存Builderクラスを内部で活用
    /// </summary>

    public class DMLQueryGenerator : IDMLQueryGenerator
    {
        private readonly IKsqlBuilder _whereBuilder;
        private readonly IKsqlBuilder _projectionBuilder;
        private readonly ILogger _logger;

        public DMLQueryGenerator(ILoggerFactory? loggerFactory = null)
        {
            _whereBuilder = new SelectBuilder();
            _projectionBuilder = new ProjectionBuilder();
            _logger = loggerFactory.CreateLoggerOrNull<DMLQueryGenerator>();
        }

        public string GenerateSelectAll(string objectName, bool isPullQuery = true)
        {
            var query = $"SELECT * FROM {objectName}";

            if (!isPullQuery)
            {
                query += " EMIT CHANGES";
            }

            _logger.LogDebug("Generated SELECT ALL query for {ObjectName}, isPullQuery: {IsPullQuery}", objectName, isPullQuery);
            _logger.LogDebug("Generated DML Query: {Query}", query);

            return query;
        }

        public string GenerateSelectWithCondition(string objectName, Expression whereExpression, bool isPullQuery = true)
        {
            var whereClause = _whereBuilder.Build(whereExpression);
            var query = $"SELECT * FROM {objectName} {whereClause}";

            if (!isPullQuery)
            {
                query += " EMIT CHANGES";
            }

            _logger.LogDebug("Generated SELECT with condition for {ObjectName}, isPullQuery: {IsPullQuery}", objectName, isPullQuery);
            _logger.LogDebug("Generated DML Query: {Query}", query);

            return query;
        }

        public string GenerateCountQuery(string objectName)
        {
            var query = $"SELECT COUNT(*) FROM {objectName}";

            _logger.LogDebug("Generated COUNT query for {ObjectName}", objectName);
            _logger.LogDebug("Generated DML Query: {Query}", query);

            return query;
        }

        public string GenerateAggregateQuery(string objectName, Expression aggregateExpression)
        {
            var selectClause = _projectionBuilder.Build(aggregateExpression);
            var query = $"{selectClause} FROM {objectName}";

            _logger.LogDebug("Generated aggregate query for {ObjectName}", objectName);
            _logger.LogDebug("Generated DML Query: {Query}", query);

            return query;
        }
    }
}