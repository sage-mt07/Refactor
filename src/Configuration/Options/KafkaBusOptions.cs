namespace KsqlDsl.Configuration.Options;

/// <summary>
/// Kafka統合バス全体の設定
/// </summary>
public record KafkaBusOptions
{
    public string BootstrapServers { get; init; } = "localhost:9092";
    public string ClientId { get; init; } = "ksql-dsl-client";
    public int RequestTimeoutMs { get; init; } = 30000;
    public int MetadataMaxAgeMs { get; init; } = 300000;
    public SecurityProtocol SecurityProtocol { get; init; } = SecurityProtocol.Plaintext;
    public SaslMechanism? SaslMechanism { get; init; }
    public string? SaslUsername { get; init; }
    public string? SaslPassword { get; init; }
    public string? SslCaLocation { get; init; }
    public string? SslCertificateLocation { get; init; }
    public string? SslKeyLocation { get; init; }
    public string? SslKeyPassword { get; init; }
}

public enum SecurityProtocol
{
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl
}

public enum SaslMechanism
{
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi,
    OAuthBearer
}