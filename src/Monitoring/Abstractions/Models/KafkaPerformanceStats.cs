using KsqlDsl.Monitoring.Metrics;

namespace KsqlDsl.Monitoring.Abstractions.Models
{
    /// <summary>
    /// 統合パフォーマンス統計
    /// </summary>
    public class KafkaPerformanceStats
    {
        public ProducerPerformanceStats ProducerStats { get; set; } = new();
        public ConsumerPerformanceStats ConsumerStats { get; set; } = new();
        public MonitoringCacheStatistics AvroStats { get; set; } = new();

        /// <summary>
        /// 全体的なスループット（メッセージ/秒）
        /// </summary>
        public double OverallThroughput => ProducerStats.ThroughputPerSecond + ConsumerStats.ThroughputPerSecond;

        /// <summary>
        /// 全体的な失敗率
        /// </summary>
        public double OverallFailureRate
        {
            get
            {
                var totalMessages = ProducerStats.TotalMessages + ConsumerStats.TotalMessages;
                var totalFailures = ProducerStats.FailedMessages + ConsumerStats.FailedMessages;
                return totalMessages > 0 ? (double)totalFailures / totalMessages : 0.0;
            }
        }

        /// <summary>
        /// 全体的な成功率
        /// </summary>
        public double OverallSuccessRate => 1.0 - OverallFailureRate;

        /// <summary>
        /// パフォーマンスサマリの生成
        /// </summary>
        public string GeneratePerformanceSummary()
        {
            return $@"Overall Performance Summary:
- Throughput: {OverallThroughput:F2} messages/sec
- Success Rate: {OverallSuccessRate:P2}
- Producer: {ProducerStats.TotalMessages:N0} messages, {ProducerStats.FailureRate:P2} failure rate
- Consumer: {ConsumerStats.TotalMessages:N0} messages, {ConsumerStats.FailureRate:P2} failure rate
- Avro Cache: {AvroStats.BaseStatistics.HitRate:P2} hit rate, {AvroStats.HealthScore:P2} health score";
        }
    }


}
