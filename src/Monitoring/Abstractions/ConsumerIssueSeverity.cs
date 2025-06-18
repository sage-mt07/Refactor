namespace KsqlDsl.Monitoring.Abstractions
{


    /// <summary>
    /// Consumer問題深刻度
    /// </summary>
    public enum ConsumerIssueSeverity
    {
        Low,
        Medium,
        High,
        Critical,
        Warning
    }
}
