namespace KsqlDsl.Monitoring.Abstractions
{



    /// <summary>
    /// プール健全性問題タイプ
    /// </summary>
    public enum PoolHealthIssueType
    {
        HighFailureRate,
        PoolExhaustion,
        ResourceLeak,
        HealthCheckFailure
    }
}
