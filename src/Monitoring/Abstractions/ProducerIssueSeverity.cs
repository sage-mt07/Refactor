namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Producer問題深刻度
    /// </summary>
    public enum ProducerIssueSeverity
    {
        Low,
        Medium,
        High,
        Critical,
        Warning
    }
}
