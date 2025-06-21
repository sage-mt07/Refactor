using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Abstractions
{
    public class SchemaGenerationStats
    {
        public int TotalProperties { get; set; }
        public int IncludedProperties { get; set; }
        public int IgnoredProperties { get; set; }
        public System.Collections.Generic.List<string> IgnoredPropertyNames { get; set; } = new();
        public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;

        public double InclusionRate => TotalProperties > 0 ? (double)IncludedProperties / TotalProperties : 0;
        public double IgnoreRate => TotalProperties > 0 ? (double)IgnoredProperties / TotalProperties : 0;

        public string GetSummary()
        {
            return $"Properties: {IncludedProperties}/{TotalProperties} included ({InclusionRate:P1}), {IgnoredProperties} ignored";
        }
    }
}
