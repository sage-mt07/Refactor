using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Options
{

    /// <summary>
    /// Consumerプール設定
    /// </summary>
    public class ConsumerPoolConfig
    {
        public int MinPoolSize { get; set; } = 1;
        public int MaxPoolSize { get; set; } = 10;
        public TimeSpan ConsumerIdleTimeout { get; set; } = TimeSpan.FromMinutes(10);
        public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromMinutes(2);
        public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromMinutes(1);
        public bool EnableRebalanceOptimization { get; set; } = true;
        public TimeSpan RebalanceTimeout { get; set; } = TimeSpan.FromSeconds(30);
    }
}
