using System.Collections.Generic;

namespace KsqlDsl.Monitoring.Metrics;

public class SchemaGenerationStats
{
    public int TotalProperties { get; set; }
    public int IncludedProperties { get; set; }
    public int IgnoredProperties { get; set; }
    public List<string> IgnoredPropertyNames { get; set; } = new();
}