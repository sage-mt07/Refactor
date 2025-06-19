using System;

namespace KsqlDsl.Monitoring.Abstractions.Models
{

    /// <summary>
    /// Consumer健全性問題
    /// </summary>
    public class ConsumerHealthIssue
    {
        public ConsumerHealthIssueType Type { get; set; }
        public string Description { get; set; } = string.Empty;
        public ConsumerIssueSeverity Severity { get; set; }
        public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
    }
}
