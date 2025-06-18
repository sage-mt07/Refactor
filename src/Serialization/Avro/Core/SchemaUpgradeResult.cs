namespace KsqlDsl.Serialization.Avro.Core
{
    public class SchemaUpgradeResult
    {
        public bool Success { get; set; }
        public string? Reason { get; set; }
        public int? NewKeySchemaId { get; set; }
        public int? NewValueSchemaId { get; set; }
    }
}
