using System;
using System.Linq.Expressions;
using KsqlDsl.Query.Abstractions;

namespace KsqlDsl.Query.Builders
{
    /// <summary>
    /// JOIN句構築ビルダー
    /// </summary>
    public class JoinBuilder : IKsqlBuilder
    {
        public KsqlBuilderType BuilderType => KsqlBuilderType.Join;

        public string Build(Expression expression)
        {
            var joinBuilder = new KsqlDsl.Ksql.KsqlJoinBuilder();
            return joinBuilder.Build(expression);
        }
    }
}