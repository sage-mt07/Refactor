using System;
using System.Collections.Generic;

namespace KsqlDsl.Core.Abstractions
{
    public class CoreDiagnostics
    {
        public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;
        public Dictionary<string, object> Configuration { get; set; } = new();
        public Dictionary<string, object> Statistics { get; set; } = new();
        public Dictionary<string, object> SystemInfo { get; set; } = new();
        public List<string> ActiveConnections { get; set; } = new();
    }
}
