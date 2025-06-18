using KsqlDsl.Monitoring.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Health
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
