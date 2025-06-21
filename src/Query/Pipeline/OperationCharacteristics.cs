using KsqlDsl.Query.Abstractions;

namespace KsqlDsl.Query.Pipeline;

internal class OperationCharacteristics
{
    public StreamTableType ResultType { get; set; }
    public bool RequiresDerivedObject { get; set; }
    public bool CanChain { get; set; }
    public bool IsAggregation { get; set; }
    public bool DependsOnAggregation { get; set; }
}
