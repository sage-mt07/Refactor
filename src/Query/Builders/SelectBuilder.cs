using System;
using System.Linq.Expressions;
using KsqlDsl.Query.Abstractions;

namespace KsqlDsl.Query.Builders
{
    /// <summary>
    /// SELECT句構築ビルダー
    /// 設計理由：既存KsqlConditionBuilderの移動・統合
    /// </summary>
    public class SelectBuilder : IKsqlBuilder
    {
        public KsqlBuilderType BuilderType => KsqlBuilderType.Where;

        public string Build(Expression expression)
        {
            var conditionBuilder = new KsqlDsl.Ksql.KsqlConditionBuilder();
            return conditionBuilder.Build(expression);
        }

        /// <summary>
        /// 条件のみ構築（WHERE プレフィックスなし）
        /// </summary>
        public string BuildCondition(Expression expression)
        {
            var conditionBuilder = new KsqlDsl.Ksql.KsqlConditionBuilder();
            return conditionBuilder.BuildCondition(expression);
        }
    }
}