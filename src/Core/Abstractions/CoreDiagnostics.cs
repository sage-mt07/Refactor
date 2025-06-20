using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
