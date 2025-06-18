using KsqlDsl.Monitoring.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Monitoring.Health
{


    /// <summary>
    /// Consumerプール健全性問題
    /// </summary>
    public class ConsumerPoolHealthIssue
    {
        public ConsumerPoolHealthIssueType Type { get; set; }
        public string Description { get; set; } = string.Empty;
        public ConsumerPoolIssueSeverity Severity { get; set; }
        public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
    }
}
