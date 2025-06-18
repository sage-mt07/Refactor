namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Consumerプール問題深刻度
    /// </summary>
    public enum ConsumerPoolIssueSeverity
    {
        Low,
        Medium,
        High,
        Critical,
        Warning
    }
}
