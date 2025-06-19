using System;

namespace KsqlDsl.Monitoring.Abstractions.Models
{
    /// <summary>
    /// プール健全性問題
    /// </summary>
    public class PoolHealthIssue
    {
        public PoolHealthIssueType Type { get; set; }
        public string Description { get; set; } = string.Empty;
        public PoolIssueSeverity Severity { get; set; }
        public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
    }
}
