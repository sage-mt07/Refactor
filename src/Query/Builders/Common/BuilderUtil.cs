using System;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Builders.Common;

/// <summary>
/// Builder共通ユーティリティ
/// 設計理由：各BuilderクラスからロジックをFactoringし、重複コード排除
/// </summary>
public static class BuilderUtil
{
    /// <summary>
    /// 式木からMemberExpressionを抽出
    /// </summary>
    public static MemberExpression? ExtractMemberExpression(Expression expression)
    {
        return expression switch
        {
            MemberExpression member => member,
            UnaryExpression unary when unary.Operand is MemberExpression member2 => member2,
            UnaryExpression unary => ExtractMemberExpression(unary.Operand),
            _ => null
        };
    }

}