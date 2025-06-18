namespace KsqlDsl.Monitoring.Abstractions
{
    /// <summary>
    /// Kafka健全性レベル
    /// </summary>
    public enum KafkaHealthLevel
    {
        Healthy,
        Warning,
        Critical
    }
}
