using System;
using System.Collections.Generic;

namespace KsqlDsl.Configuration.Abstractions
{

    /// <summary>
    /// Producerプール設定
    /// </summary>
    public class ProducerPoolConfig
    {
        public int MinPoolSize { get; set; } = 1;
        public int MaxPoolSize { get; set; } = 10;
        public TimeSpan ProducerIdleTimeout { get; set; } = TimeSpan.FromMinutes(5);
        public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromMinutes(2);
        public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromMinutes(1);
        public bool EnableDynamicScaling { get; set; } = true;
        public double ScaleUpThreshold { get; set; } = 0.8;
        public double ScaleDownThreshold { get; set; } = 0.3;
        public Dictionary<string, object> AdditionalConfig { get; set; } = new();
        public int LingerMs { get; set; } = 0;           // ✅ 追加
        public int BatchSize { get; set; } = 16384;      // ✅ 追加
    }

}
