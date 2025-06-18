using System.Collections.Generic;

namespace KsqlDsl.Serialization.Avro.Core;



public class AvroSchema
{
    public string Type { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string? Namespace { get; set; }
    public string? Doc { get; set; }
    public List<AvroField> Fields { get; set; } = new();
}