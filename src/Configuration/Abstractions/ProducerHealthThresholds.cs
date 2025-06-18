namespace KsqlDsl.Configuration.Abstractions
{
    /// <summary>
    /// Producer健全性閾値
    /// </summary>
    public class ProducerHealthThresholds
    {
        public long MaxAverageLatencyMs { get; set; } = 100;
        public long CriticalLatencyMs { get; set; } = 1000;
        public double MaxFailureRate { get; set; } = 0.1;
        public double CriticalFailureRate { get; set; } = 0.2;
        public long MinThroughputPerSecond { get; set; } = 10;
    }
}
