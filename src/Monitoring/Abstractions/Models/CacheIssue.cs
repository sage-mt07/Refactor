using System;

namespace KsqlDsl.Monitoring.Abstractions.Models
{


    public class CacheIssue
    {
        public CacheIssueType Type { get; set; }
        public string Description { get; set; } = string.Empty;
        public CacheIssueSeverity Severity { get; set; }
        public Type? AffectedEntityType { get; set; }
        public string? Recommendation { get; set; }
    }
}
