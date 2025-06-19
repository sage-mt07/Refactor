using System;

namespace KsqlDsl.Monitoring.Abstractions.Models
{
    /// <summary>
    /// Producer健全性問題
    /// </summary>
    public class ProducerHealthIssue
    {
        public ProducerHealthIssueType Type { get; set; }
        public string Description { get; set; } = string.Empty;
        public ProducerIssueSeverity Severity { get; set; }
        public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
    }
}
