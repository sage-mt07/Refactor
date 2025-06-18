namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Kafka健全性問題タイプ
    /// </summary>
    public enum KafkaHealthIssueType
    {
        HealthCheckFailure,
        ProducerIssue,
        ConsumerIssue,
        AvroIssue,
        PerformanceIssue,
        ConfigurationIssue
    }
}
