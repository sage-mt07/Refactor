using System;
using System.Linq.Expressions;
using KsqlDsl.Query.Abstractions;

namespace KsqlDsl.Query.Builders
{
    /// <summary>
    /// 射影（SELECT列）構築ビルダー
    /// </summary>
    public class ProjectionBuilder : IKsqlBuilder
    {
        public KsqlBuilderType BuilderType => KsqlBuilderType.Projection;

        public string Build(Expression expression)
        {
            var projectionBuilder = new KsqlDsl.Ksql.KsqlProjectionBuilder();
            return projectionBuilder.Build(expression);
        }
    }
}
