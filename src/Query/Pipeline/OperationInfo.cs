using System.Linq.Expressions;

namespace KsqlDsl.Query.Pipeline;

internal class OperationInfo
{
    public string MethodName { get; set; } = string.Empty;
    public MethodCallExpression Expression { get; set; } = null!;
    public OperationCharacteristics Characteristics { get; set; } = null!;
    public int Order { get; set; }
}
