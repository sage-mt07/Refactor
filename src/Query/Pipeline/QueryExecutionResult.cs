using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Query.Pipeline
{
    public class QueryExecutionResult
    {
        public bool Success { get; set; }
        public string TargetObject { get; set; } = string.Empty;
        public object? Data { get; set; }
        public DateTime ExecutedAt { get; set; }
        public string? ErrorMessage { get; set; }
    }
}
