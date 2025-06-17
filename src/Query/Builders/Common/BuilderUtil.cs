using System;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Builders.Common
{
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

        /// <summary>
        /// 式木からLambdaExpressionを抽出
        /// </summary>
        public static LambdaExpression? ExtractLambdaExpression(Expression expression)
        {
            return expression switch
            {
                LambdaExpression lambda => lambda,
                UnaryExpression { Operand: LambdaExpression lambda } => lambda,
                _ => null
            };
        }

        /// <summary>
        /// メンバー名を取得（パラメータプレフィックス付き/なし）
        /// </summary>
        public static string GetMemberName(MemberExpression member, bool includeParameterPrefix = false)
        {
            if (includeParameterPrefix && member.Expression is ParameterExpression param)
            {
                return $"{param.Name}.{member.Member.Name}";
            }
            return member.Member.Name;
        }

        /// <summary>
        /// KSQL用SQL演算子変換
        /// </summary>
        public static string GetSqlOperator(ExpressionType nodeType) => nodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            _ => throw new NotSupportedException($"Unsupported operator: {nodeType}")
        };

        /// <summary>
        /// 集約関数名の正規化（C# → KSQL）
        /// </summary>
        public static string TransformMethodName(string methodName)
        {
            return methodName.ToUpper() switch
            {
                "LATESTBYOFFSET" => "LATEST_BY_OFFSET",
                "EARLIESTBYOFFSET" => "EARLIEST_BY_OFFSET",
                "COLLECTLIST" => "COLLECT_LIST",
                "COLLECTSET" => "COLLECT_SET",
                "AVERAGE" => "AVG", // KSQL uses AVG instead of AVERAGE
                _ => methodName.ToUpper()
            };
        }

        /// <summary>
        /// 集約関数判定
        /// </summary>
        public static bool IsAggregateFunction(string methodName)
        {
            var upper = methodName.ToUpper();
            return upper switch
            {
                "SUM" or "COUNT" or "MAX" or "MIN" or "AVG" or "AVERAGE" or
                "LATESTBYOFFSET" or "EARLIESTBYOFFSET" or
                "COLLECTLIST" or "COLLECTSET" or
                "COLLECT_LIST" or "COLLECT_SET" or
                "LATEST_BY_OFFSET" or "EARLIEST_BY_OFFSET" => true,
                _ => false
            };
        }

        /// <summary>
        /// KSQL関数名変換（プロジェクション用）
        /// </summary>
        public static string TransformProjectionMethodName(string methodName)
        {
            return methodName.ToUpper() switch
            {
                "TOSTRING" => "CAST",
                "TOLOWER" => "LCASE",
                "TOUPPER" => "UCASE",
                "SUBSTRING" => "SUBSTRING",
                "LENGTH" => "LEN",
                "TRIM" => "TRIM",
                "REPLACE" => "REPLACE",
                _ => methodName.ToUpper()
            };
        }

        /// <summary>
        /// 定数値のKSQL文字列化
        /// </summary>
        public static string FormatConstantValue(object? value, Type type)
        {
            if (value == null)
                return "NULL";

            return type switch
            {
                Type t when t == typeof(string) => $"'{value}'",
                Type t when t == typeof(bool) => value.ToString()?.ToLower() ?? "false",
                Type t when t == typeof(char) => $"'{value}'",
                Type t when t == typeof(DateTime) => $"'{((DateTime)value):yyyy-MM-dd HH:mm:ss}'",
                Type t when t == typeof(DateTimeOffset) => $"'{((DateTimeOffset)value):yyyy-MM-dd HH:mm:ss}'",
                Type t when t == typeof(Guid) => $"'{value}'",
                _ => value.ToString() ?? "NULL"
            };
        }

        /// <summary>
        /// TimeSpan→KSQL時間単位変換
        /// </summary>
        public static string FormatTimeSpan(TimeSpan timeSpan)
        {
            if (timeSpan.TotalDays >= 1)
                return $"{(int)timeSpan.TotalDays} DAYS";
            if (timeSpan.TotalHours >= 1)
                return $"{(int)timeSpan.TotalHours} HOURS";
            if (timeSpan.TotalMinutes >= 1)
                return $"{(int)timeSpan.TotalMinutes} MINUTES";
            if (timeSpan.TotalSeconds >= 1)
                return $"{(int)timeSpan.TotalSeconds} SECONDS";

            return "0 SECONDS";
        }

        /// <summary>
        /// Boolean式の正規化（括弧付き条件への変換）
        /// </summary>
        public static string NormalizeBooleanExpression(string memberName, bool expectedValue = true)
        {
            return $"({memberName} = {(expectedValue ? "true" : "false")})";
        }

        /// <summary>
        /// Nullable Boolean .Value アクセスの検出
        /// </summary>
        public static bool IsNullableBooleanValueAccess(MemberExpression member, out MemberExpression? innerMember)
        {
            innerMember = null;

            if (member.Member.Name == "Value" &&
                member.Expression is MemberExpression inner &&
                inner.Type == typeof(bool?))
            {
                innerMember = inner;
                return true;
            }

            return false;
        }

        /// <summary>
        /// 式木デバッグ用文字列生成
        /// </summary>
        public static string GetExpressionDebugInfo(Expression expression)
        {
            return $"Type: {expression.NodeType}, ExprType: {expression.Type.Name}, ToString: {expression}";
        }
    }
}