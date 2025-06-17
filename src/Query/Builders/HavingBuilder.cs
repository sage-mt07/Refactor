using System;
using System.Linq.Expressions;
using KsqlDsl.Query.Abstractions;

namespace KsqlDsl.Query.Builders
{
    /// <summary>
    /// HAVING句構築ビルダー
    /// </summary>
    public class HavingBuilder : IKsqlBuilder
    {
        public KsqlBuilderType BuilderType => KsqlBuilderType.Having;

        public string Build(Expression expression)
        {
            var havingBuilder = new KsqlDsl.Ksql.KsqlHavingBuilder();
            return havingBuilder.Build(expression);
        }
    }
}