using System;
using System.Linq.Expressions;
using KsqlDsl.Query.Abstractions;

namespace KsqlDsl.Query.Builders
{
    /// <summary>
    /// WINDOW句構築ビルダー
    /// </summary>
    public class WindowBuilder : IKsqlBuilder
    {
        public KsqlBuilderType BuilderType => KsqlBuilderType.Window;

        public string Build(Expression expression)
        {
            var windowBuilder = new KsqlDsl.Ksql.KsqlWindowBuilder();
            return windowBuilder.Build(expression);
        }
    }
}