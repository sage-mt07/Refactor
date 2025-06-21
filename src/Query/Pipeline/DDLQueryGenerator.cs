using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Query.Abstractions;
using KsqlDsl.Query.Builders;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace KsqlDsl.Query.Pipeline;

/// <summary>
/// DDLクエリ生成器 - CREATE STREAM/TABLE文の生成
/// 設計理由：query_redesign.mdの新アーキテクチャに準拠
/// </summary>


internal class DDLQueryGenerator : IDDLQueryGenerator
{
    private readonly IKsqlBuilder _whereBuilder;
    private readonly IKsqlBuilder _projectionBuilder;
    private readonly IKsqlBuilder _groupByBuilder;
    private readonly StreamTableAnalyzer _analyzer;
    private readonly ILogger _logger;

    public DDLQueryGenerator(ILoggerFactory? loggerFactory = null)
    {
        _whereBuilder = new SelectBuilder();
        _projectionBuilder = new ProjectionBuilder();
        _groupByBuilder = new GroupByBuilder();
        _analyzer = new StreamTableAnalyzer();
        _logger = loggerFactory.CreateLoggerOrNull<DDLQueryGenerator>();
    }

    public string GenerateCreateStream(string streamName, string topicName, EntityModel entityModel)
    {
        var columns = GenerateColumnDefinitions(entityModel);
        var query = $"CREATE STREAM {streamName} ({columns}) WITH (KAFKA_TOPIC='{topicName}', VALUE_FORMAT='AVRO')";

        _logger.LogDebug("Generated CREATE STREAM: {StreamName} for topic: {TopicName}", streamName, topicName);
        return query;
    }

    public string GenerateCreateTable(string tableName, string topicName, EntityModel entityModel)
    {
        var columns = GenerateColumnDefinitions(entityModel);
        var keyColumns = string.Join(", ", entityModel.KeyProperties.Select(p => p.Name.ToUpper()));
        var query = $"CREATE TABLE {tableName} ({columns}) WITH (KAFKA_TOPIC='{topicName}', VALUE_FORMAT='AVRO', KEY='{keyColumns}')";

        _logger.LogDebug("Generated CREATE TABLE: {TableName} for topic: {TopicName} with keys: {KeyColumns}",
            tableName, topicName, keyColumns);
        return query;
    }

    public string GenerateCreateStreamAs(string streamName, string baseObject, Expression linqExpression)
    {
        var analysis = _analyzer.AnalyzeExpression(linqExpression);
        var selectClause = GenerateSelectClause(linqExpression, analysis);
        var whereClause = GenerateWhereClause(linqExpression, analysis);

        var query = new StringBuilder($"CREATE STREAM {streamName} AS SELECT {selectClause} FROM {baseObject}");

        if (!string.IsNullOrEmpty(whereClause))
        {
            query.Append($" {whereClause}");
        }

        var finalQuery = query.ToString();
        _logger.LogDebug("Generated CREATE STREAM AS: {StreamName} from {BaseObject}", streamName, baseObject);
        _logger.LogDebug("Generated DDL Query: {Query}", finalQuery);

        return finalQuery;
    }

    public string GenerateCreateTableAs(string tableName, string baseObject, Expression linqExpression)
    {
        var analysis = _analyzer.AnalyzeExpression(linqExpression);
        var selectClause = GenerateSelectClause(linqExpression, analysis);
        var whereClause = GenerateWhereClause(linqExpression, analysis);
        var groupByClause = GenerateGroupByClause(linqExpression, analysis);

        var query = new StringBuilder($"CREATE TABLE {tableName} AS SELECT {selectClause} FROM {baseObject}");

        if (!string.IsNullOrEmpty(whereClause))
        {
            query.Append($" {whereClause}");
        }

        if (!string.IsNullOrEmpty(groupByClause))
        {
            query.Append($" {groupByClause}");
        }

        var finalQuery = query.ToString();
        _logger.LogDebug("Generated CREATE TABLE AS: {TableName} from {BaseObject}", tableName, baseObject);
        _logger.LogDebug("Generated DDL Query: {Query}", finalQuery);

        return finalQuery;
    }

    private string GenerateColumnDefinitions(EntityModel entityModel)
    {
        var columns = new List<string>();

        foreach (var property in entityModel.EntityType.GetProperties())
        {
            if (property.GetCustomAttribute<KafkaIgnoreAttribute>() != null)
                continue;

            var columnName = property.Name.ToUpper();
            var ksqlType = MapToKsqlType(property.PropertyType);
            columns.Add($"{columnName} {ksqlType}");
        }

        var columnDefinitions = string.Join(", ", columns);
        _logger.LogDebug("Generated column definitions: {ColumnDefinitions}", columnDefinitions);

        return columnDefinitions;
    }

    private string MapToKsqlType(Type propertyType)
    {
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        return underlyingType switch
        {
            Type t when t == typeof(int) => "INTEGER",
            Type t when t == typeof(long) => "BIGINT",
            Type t when t == typeof(double) => "DOUBLE",
            Type t when t == typeof(decimal) => "DECIMAL",
            Type t when t == typeof(string) => "VARCHAR",
            Type t when t == typeof(bool) => "BOOLEAN",
            Type t when t == typeof(DateTime) => "TIMESTAMP",
            Type t when t == typeof(DateTimeOffset) => "TIMESTAMP",
            Type t when t == typeof(Guid) => "VARCHAR",
            Type t when t == typeof(byte[]) => "BYTES",
            _ => "VARCHAR"
        };
    }

    private string GenerateSelectClause(Expression expression, ExpressionAnalysisResult analysis)
    {
        var selectCall = analysis.MethodCalls.LastOrDefault(mc => mc.Method.Name == "Select");

        if (selectCall != null)
        {
            var selectExpression = UnwrapLambda(selectCall.Arguments[1]);
            if (selectExpression != null)
            {
                return _projectionBuilder.Build(selectExpression).Replace("SELECT ", "");
            }
        }

        return "*";
    }

    private string GenerateWhereClause(Expression expression, ExpressionAnalysisResult analysis)
    {
        var whereCall = analysis.MethodCalls.FirstOrDefault(mc => mc.Method.Name == "Where");

        if (whereCall != null)
        {
            var whereExpression = UnwrapLambda(whereCall.Arguments[1]);
            if (whereExpression != null)
            {
                return _whereBuilder.Build(whereExpression);
            }
        }

        return string.Empty;
    }

    private string GenerateGroupByClause(Expression expression, ExpressionAnalysisResult analysis)
    {
        var groupByCall = analysis.MethodCalls.FirstOrDefault(mc => mc.Method.Name == "GroupBy");

        if (groupByCall != null)
        {
            var groupByExpression = UnwrapLambda(groupByCall.Arguments[1]);
            if (groupByExpression != null)
            {
                return _groupByBuilder.Build(groupByExpression);
            }
        }

        return string.Empty;
    }

    private Expression? UnwrapLambda(Expression expression)
    {
        return expression switch
        {
            LambdaExpression lambda => lambda.Body,
            UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression lambda } => lambda.Body,
            _ => null
        };
    }
}




// StreamTableType enumは KsqlDsl.Query.Abstractions.StreamTableType を使用