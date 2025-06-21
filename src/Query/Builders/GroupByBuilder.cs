using KsqlDsl.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace KsqlDsl.Query.Builders;

/// <summary>
/// GROUP BY句構築ビルダー - 本体実装版
/// 設計理由：旧KsqlGroupByBuilderへの中継を排除し、直接実装に移行
/// </summary>
public class GroupByBuilder : IKsqlBuilder
{
    public KsqlBuilderType BuilderType => KsqlBuilderType.GroupBy;

    public string Build(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        var visitor = new GroupByVisitor();
        visitor.Visit(expression);
        var result = visitor.ToString();

        if (string.IsNullOrEmpty(result))
        {
            throw new InvalidOperationException("Unable to extract GROUP BY keys from expression");
        }

        return "GROUP BY " + result;
    }

    /// <summary>
    /// GROUP BY専用ExpressionVisitor
    /// </summary>
    private class GroupByVisitor : ExpressionVisitor
    {
        private readonly StringBuilder _sb = new();

        protected override Expression VisitNew(NewExpression node)
        {
            var keys = new List<string>();

            // Extract keys from Arguments with UnaryExpression support
            foreach (var arg in node.Arguments)
            {
                var memberName = ExtractMemberName(arg);
                if (!string.IsNullOrEmpty(memberName))
                {
                    keys.Add(memberName);
                }
            }

            // Build the result string
            for (int i = 0; i < keys.Count; i++)
            {
                _sb.Append(keys[i]);
                if (i < keys.Count - 1)
                    _sb.Append(", ");
            }

            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            _sb.Append(node.Member.Name);
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            // Handle Convert operations by extracting the underlying member
            if (node.NodeType == ExpressionType.Convert || node.NodeType == ExpressionType.ConvertChecked)
            {
                return Visit(node.Operand);
            }

            return base.VisitUnary(node);
        }

        private string? ExtractMemberName(Expression expr)
        {
            return expr switch
            {
                MemberExpression member => member.Member.Name,
                UnaryExpression unary when unary.Operand is MemberExpression member2 => member2.Member.Name,
                UnaryExpression unary => ExtractMemberName(unary.Operand),
                _ => null
            };
        }

        public override string ToString()
        {
            return _sb.ToString();
        }
    }
}