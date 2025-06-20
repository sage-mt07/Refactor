using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions
{
    public class CoreHealthReport
    {
        public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;
        public CoreHealthStatus Status { get; set; }
        public Dictionary<string, object> ComponentStatus { get; set; } = new();
        public List<string> Issues { get; set; } = new();
        public TimeSpan CheckDuration { get; set; }
    }
}
