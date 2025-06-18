using KsqlDsl.Query.Abstructions;

namespace KsqlDsl.Query.Linq;



internal class InferenceResult
{
    public StreamTableType InferredType { get; set; }
    public bool IsExplicitlyDefined { get; set; }
    public string Reason { get; set; } = "";
}