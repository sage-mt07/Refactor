namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Consumerプール健全性問題タイプ
    /// </summary>
    public enum ConsumerPoolHealthIssueType
    {
        HighFailureRate,
        PoolExhaustion,
        RebalanceFailure,
        HealthCheckFailure
    }
}
