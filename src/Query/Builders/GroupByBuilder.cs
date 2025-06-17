using System;
using System.Linq.Expressions;
using KsqlDsl.Query.Abstractions;

namespace KsqlDsl.Query.Builders
{
    /// <summary>
    /// GROUP BY句構築ビルダー
    /// </summary>
    public class GroupByBuilder : IKsqlBuilder
    {
        public KsqlBuilderType BuilderType => KsqlBuilderType.GroupBy;

        public string Build(Expression expression)
        {
            return KsqlDsl.Ksql.KsqlGroupByBuilder.Build(expression);
        }
    }
}