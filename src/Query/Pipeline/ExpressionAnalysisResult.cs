using System.Collections.Generic;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Pipeline;

internal class ExpressionAnalysisResult
{
    public List<MethodCallExpression> MethodCalls { get; set; } = new();
    public bool HasGroupBy { get; set; }
    public bool HasAggregation { get; set; }
    public bool HasJoin { get; set; }
    public bool RequiresStreamOutput { get; set; } = true;
    public bool RequiresTableOutput { get; set; }
    public List<OperationInfo> OperationChain { get; set; } = new();
}
