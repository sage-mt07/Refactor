namespace KsqlDsl.Monitoring.Abstractions
{

    /// <summary>
    /// Consumerプール健全性レベル
    /// </summary>
    public enum ConsumerPoolHealthLevel
    {
        Healthy,
        Warning,
        Critical
    }
}
