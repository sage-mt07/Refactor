using KsqlDsl.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace KsqlDsl.Query.Builders;

/// <summary>
/// WHERE句構築ビルダー - 本体実装版
/// 設計理由：旧KsqlConditionBuilderへの中継を排除し、直接実装に移行
/// </summary>
public class SelectBuilder : IKsqlBuilder
{
    public KsqlBuilderType BuilderType => KsqlBuilderType.Where;

    public string Build(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        var visitor = new WhereExpressionVisitor(includeParameterPrefix: false);
        visitor.Visit(expression);
        return "WHERE " + visitor.ToString();
    }

    /// <summary>
    /// 条件のみ構築（WHERE プレフィックスなし）
    /// </summary>
    public string BuildCondition(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        var visitor = new WhereExpressionVisitor(includeParameterPrefix: true);
        visitor.Visit(expression);
        return visitor.ToString();
    }

    /// <summary>
    /// WHERE句専用ExpressionVisitor
    /// </summary>
    private class WhereExpressionVisitor : ExpressionVisitor
    {
        private readonly StringBuilder _sb = new();
        private readonly bool _includeParameterPrefix;

        public WhereExpressionVisitor(bool includeParameterPrefix)
        {
            _includeParameterPrefix = includeParameterPrefix;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            // Handle composite key joins: new { a.Id, a.Type } equals new { b.Id, b.Type }
            if (node.NodeType == ExpressionType.Equal &&
                node.Left is NewExpression leftNew &&
                node.Right is NewExpression rightNew)
            {
                BuildCompositeKeyCondition(leftNew, rightNew);
                return node;
            }

            // Special case: bool member == true (avoid double bool processing)
            if (node.NodeType == ExpressionType.Equal &&
                node.Left is MemberExpression leftMember &&
                (leftMember.Type == typeof(bool) || leftMember.Type == typeof(bool?)) &&
                node.Right is ConstantExpression rightConstant &&
                rightConstant.Value is bool boolValue)
            {
                string memberName = GetMemberName(leftMember);
                _sb.Append("(");
                _sb.Append(memberName);
                _sb.Append(boolValue ? " = true" : " = false");
                _sb.Append(")");
                return node;
            }

            // Handle regular binary expressions
            _sb.Append("(");
            Visit(node.Left);
            _sb.Append(" " + GetSqlOperator(node.NodeType) + " ");
            Visit(node.Right);
            _sb.Append(")");
            return node;
        }

        private void BuildCompositeKeyCondition(NewExpression leftNew, NewExpression rightNew)
        {
            if (leftNew.Arguments.Count != rightNew.Arguments.Count)
            {
                throw new InvalidOperationException("Composite key expressions must have the same number of properties");
            }

            if (leftNew.Arguments.Count == 0)
            {
                throw new InvalidOperationException("Composite key expressions must have at least one property");
            }

            var conditions = new List<string>();

            for (int i = 0; i < leftNew.Arguments.Count; i++)
            {
                var leftMemberName = ExtractMemberName(leftNew.Arguments[i]);
                var rightMemberName = ExtractMemberName(rightNew.Arguments[i]);

                if (leftMemberName == null || rightMemberName == null)
                {
                    throw new InvalidOperationException($"Unable to extract member names from composite key at index {i}");
                }

                conditions.Add($"{leftMemberName} = {rightMemberName}");
            }

            // Join all conditions with AND, wrap in parentheses for complex expressions
            if (conditions.Count == 1)
            {
                _sb.Append(conditions[0]);
            }
            else
            {
                _sb.Append("(");
                _sb.Append(string.Join(" AND ", conditions));
                _sb.Append(")");
            }
        }

        private string? ExtractMemberName(Expression expression)
        {
            return expression switch
            {
                MemberExpression member => GetMemberName(member),
                UnaryExpression unary => ExtractMemberName(unary.Operand),
                _ => null
            };
        }

        private string GetMemberName(MemberExpression member)
        {
            if (_includeParameterPrefix && member.Expression is ParameterExpression param)
            {
                return $"{param.Name}.{member.Member.Name}";
            }
            return member.Member.Name;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.Not:
                    // Handle nullable bool negation: !o.IsProcessed.Value → "(IsProcessed = false)"
                    if (node.Operand is MemberExpression member &&
                        member.Member.Name == "Value" &&
                        member.Expression is MemberExpression innerMember &&
                        innerMember.Type == typeof(bool?))
                    {
                        string memberName = GetMemberName(innerMember);
                        _sb.Append("(");
                        _sb.Append(memberName);
                        _sb.Append(" = false");
                        _sb.Append(")");
                        return node;
                    }
                    // Handle regular boolean negation: !o.IsActive → "(IsActive = false)"
                    else if (node.Operand is MemberExpression regularMember &&
                        (regularMember.Type == typeof(bool) || regularMember.Type == typeof(bool?)))
                    {
                        string memberName = GetMemberName(regularMember);
                        _sb.Append("(");
                        _sb.Append(memberName);
                        _sb.Append(" = false");
                        _sb.Append(")");
                        return node;
                    }
                    break;

                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    // Handle type conversions by visiting the operand
                    return Visit(node.Operand);
            }

            // Default handling for other unary expressions
            return base.VisitUnary(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Handle nullable bool .Value access: o.IsProcessed.Value → "(IsProcessed = true)"
            if (node.Member.Name == "Value" &&
                node.Expression is MemberExpression innerMember &&
                innerMember.Type == typeof(bool?))
            {
                string memberName = GetMemberName(innerMember);
                _sb.Append("(");
                _sb.Append(memberName);
                _sb.Append(" = true");
                _sb.Append(")");
                return node;
            }

            // Build the member name with or without parameter prefix
            string finalMemberName = GetMemberName(node);

            // Handle bool and nullable bool properties explicitly with parentheses
            if (node.Type == typeof(bool) || node.Type == typeof(bool?))
            {
                _sb.Append("(");
                _sb.Append(finalMemberName);
                _sb.Append(" = true");
                _sb.Append(")");
            }
            else
            {
                _sb.Append(finalMemberName);
            }

            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Type == typeof(string))
                _sb.Append($"'{node.Value}'");
            else if (node.Type == typeof(bool))
                _sb.Append(node.Value?.ToString()?.ToLower() ?? "false"); // null安全性を追加
            else
                _sb.Append(node.Value ?? "NULL"); // null安全性を追加
            return node;
        }

        private static string GetSqlOperator(ExpressionType nodeType) => nodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            _ => throw new NotSupportedException($"Unsupported operator: {nodeType}")
        };

        public override string ToString()
        {
            return _sb.ToString();
        }
    }
}