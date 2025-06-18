using KsqlDsl.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace KsqlDsl.Query.Builders
{
    /// <summary>
    /// JOIN句構築ビルダー - 本体実装版
    /// 設計理由：旧KsqlJoinBuilderへの中継を排除し、直接実装に移行
    /// </summary>
    public class JoinBuilder : IKsqlBuilder
    {
        public KsqlBuilderType BuilderType => KsqlBuilderType.Join;

        public string Build(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            var joinCall = FindJoinCall(expression);
            if (joinCall == null)
                return "UNSUPPORTED JOIN";

            try
            {
                return BuildJoinQuery(joinCall);
            }
            catch (Exception ex)
            {
                return $"/* JOIN構築エラー: {ex.Message} */";
            }
        }

        private string BuildJoinQuery(MethodCallExpression joinCall)
        {
            // Join arguments: outer, inner, outerKeySelector, innerKeySelector, resultSelector
            if (joinCall.Arguments.Count < 5)
            {
                throw new InvalidOperationException("Join expression must have 5 arguments");
            }

            var outerKeySelector = ExtractLambdaExpression(joinCall.Arguments[2]);
            var innerKeySelector = ExtractLambdaExpression(joinCall.Arguments[3]);
            var resultSelector = ExtractLambdaExpression(joinCall.Arguments[4]);

            if (outerKeySelector == null || innerKeySelector == null || resultSelector == null)
            {
                throw new InvalidOperationException("Unable to extract lambda expressions from Join");
            }

            var outerKeys = ExtractJoinKeys(outerKeySelector.Body);
            var innerKeys = ExtractJoinKeys(innerKeySelector.Body);
            var projections = ExtractProjection(resultSelector.Body);

            if (outerKeys.Count != innerKeys.Count || outerKeys.Count == 0)
            {
                throw new InvalidOperationException("Join keys mismatch or empty");
            }

            // Build JOIN condition
            var conditions = new StringBuilder();
            var outerAlias = outerKeySelector.Parameters.FirstOrDefault()?.Name ?? "o";
            var innerAlias = innerKeySelector.Parameters.FirstOrDefault()?.Name ?? "c";

            for (int i = 0; i < outerKeys.Count; i++)
            {
                if (i > 0) conditions.Append(" AND ");
                conditions.Append($"{outerAlias}.{outerKeys[i]} = {innerAlias}.{innerKeys[i]}");
            }

            // Extract type names
            var outerTypeArg = joinCall.Arguments[0].Type.GetGenericArguments().FirstOrDefault();
            var innerTypeArg = joinCall.Arguments[1].Type.GetGenericArguments().FirstOrDefault();

            if (outerTypeArg == null || innerTypeArg == null)
            {
                throw new InvalidOperationException("Unable to resolve type arguments from Join");
            }

            var outerType = outerTypeArg.Name;
            var innerType = innerTypeArg.Name;

            // Build complete JOIN query
            return $"SELECT {string.Join(", ", projections)} FROM {outerType} {outerAlias} JOIN {innerType} {innerAlias} ON {conditions}";
        }

        private MethodCallExpression? FindJoinCall(Expression expr)
        {
            switch (expr)
            {
                case MethodCallExpression mce when mce.Method.Name == "Join":
                    return mce;
                case LambdaExpression le:
                    return FindJoinCall(le.Body);
                case UnaryExpression ue:
                    return FindJoinCall(ue.Operand);
                case InvocationExpression ie:
                    return FindJoinCall(ie.Expression);
                case MemberInitExpression mie:
                    foreach (var binding in mie.Bindings)
                    {
                        if (binding is MemberAssignment ma)
                        {
                            var inner = FindJoinCall(ma.Expression);
                            if (inner != null) return inner;
                        }
                    }
                    break;
                case NewExpression ne:
                    foreach (var arg in ne.Arguments)
                    {
                        var inner = FindJoinCall(arg);
                        if (inner != null) return inner;
                    }
                    break;
            }
            return null;
        }

        private List<string> ExtractJoinKeys(Expression? expr)
        {
            var keys = new List<string>();

            if (expr == null)
                return keys;

            switch (expr)
            {
                case NewExpression newExpr:
                    foreach (var arg in newExpr.Arguments)
                    {
                        var member = ExtractMemberExpression(arg);
                        if (member != null) keys.Add(member.Member.Name);
                    }
                    break;
                case MemberExpression memberExpr:
                    keys.Add(memberExpr.Member.Name);
                    break;
            }

            return keys;
        }

        private static LambdaExpression? ExtractLambdaExpression(Expression expr)
        {
            return expr switch
            {
                UnaryExpression unary when unary.Operand is LambdaExpression lambda => lambda,
                LambdaExpression lambda => lambda,
                _ => null
            };
        }

        private static MemberExpression? ExtractMemberExpression(Expression expr)
        {
            return expr switch
            {
                MemberExpression m => m,
                UnaryExpression u when u.Operand is MemberExpression m => m,
                _ => null
            };
        }

        private List<string> ExtractProjection(Expression? expr)
        {
            var props = new List<string>();

            if (expr == null)
                return props;

            if (expr is NewExpression newExpr)
            {
                foreach (var arg in newExpr.Arguments)
                {
                    if (arg is MemberExpression memberExpr)
                    {
                        string? alias = null;
                        if (memberExpr.Expression is ParameterExpression pe)
                        {
                            alias = pe.Name;
                        }
                        else if (memberExpr.Expression is MemberExpression me && me.Expression is ParameterExpression mpe)
                        {
                            alias = mpe.Name;
                        }

                        if (string.IsNullOrEmpty(alias))
                        {
                            throw new InvalidOperationException("Unable to resolve alias for projection.");
                        }

                        props.Add($"{alias}.{memberExpr.Member.Name}");
                    }
                }
            }

            return props;
        }
    }
}