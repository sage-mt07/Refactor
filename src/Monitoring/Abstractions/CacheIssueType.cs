namespace KsqlDsl.Monitoring.Abstractions
{

    public enum CacheIssueType
    {
        LowHitRate,
        ExcessiveMisses,
        StaleSchemas,
        MemoryPressure,
        SchemaVersionMismatch
    }
}
