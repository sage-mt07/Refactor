namespace KsqlDsl.Monitoring.Abstractions
{

    /// <summary>
    /// Producer健全性問題タイプ
    /// </summary>
    public enum ProducerHealthIssueType
    {
        HighFailureRate,
        HighLatency,
        LowThroughput,
        PoolExhaustion,
        ConfigurationError,
        HealthCheckFailure
    }
}
