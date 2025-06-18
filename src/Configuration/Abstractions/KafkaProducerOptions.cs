namespace KsqlDsl.Configuration.Abstractions;


/// <summary>
/// Kafka Producer個別設定
/// </summary>
public record KafkaProducerOptions
{
    public Acks Acks { get; init; } = Acks.Leader;
    public int RetryBackoffMs { get; init; } = 100;
    public int MessageTimeoutMs { get; init; } = 300000;
    public CompressionType CompressionType { get; init; } = CompressionType.None;
    public int BatchSize { get; init; } = 16384;
    public int LingerMs { get; init; } = 0;
    public int BufferMemory { get; init; } = 33554432;
    public int MaxInFlightRequestsPerConnection { get; init; } = 5;
    public bool EnableIdempotence { get; init; } = false;
    public int MaxRequestSize { get; init; } = 1048576;
    public string? TransactionalId { get; init; }
    public int TransactionTimeoutMs { get; init; } = 60000;
}

public enum Acks
{
    None = 0,
    Leader = 1,
    All = -1
}

public enum CompressionType
{
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd
}