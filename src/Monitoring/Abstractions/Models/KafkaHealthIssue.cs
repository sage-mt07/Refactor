using KsqlDsl.Monitoring.Abstractions;
using System;

namespace KsqlDsl.Monitoring.Abstractions.Models
{

    /// <summary>
    /// Kafka健全性問題
    /// </summary>
    public class KafkaHealthIssue
    {
        public KafkaHealthIssueType Type { get; set; }
        public string Description { get; set; } = string.Empty;
        public KafkaIssueSeverity Severity { get; set; }
        public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
        public string? Recommendation { get; set; }
    }
}
