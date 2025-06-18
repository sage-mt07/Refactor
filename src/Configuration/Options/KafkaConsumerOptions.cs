namespace KsqlDsl.Configuration.Options;


/// <summary>
/// Kafka Consumer個別設定
/// </summary>
public record KafkaConsumerOptions
{
    public string GroupId { get; init; } = "ksql-dsl-group";
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Latest;
    public bool EnableAutoCommit { get; init; } = true;
    public int AutoCommitIntervalMs { get; init; } = 5000;
    public int SessionTimeoutMs { get; init; } = 30000;
    public int HeartbeatIntervalMs { get; init; } = 3000;
    public int MaxPollIntervalMs { get; init; } = 300000;
    public int FetchMinBytes { get; init; } = 1;
    public int FetchMaxWaitMs { get; init; } = 500;
    public int MaxPartitionFetchBytes { get; init; } = 1048576;
    public string? GroupInstanceId { get; init; }
    public IsolationLevel IsolationLevel { get; init; } = IsolationLevel.ReadUncommitted;
}

public enum AutoOffsetReset
{
    Latest,
    Earliest,
    None
}

public enum IsolationLevel
{
    ReadUncommitted,
    ReadCommitted
}