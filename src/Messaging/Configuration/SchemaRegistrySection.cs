using System.Collections.Generic;

namespace KsqlDsl.Messaging.Configuration;

/// <summary>
/// Schema Registry設定（Confluent.Kafka仕様準拠）
/// </summary>
public class SchemaRegistrySection
{
    /// <summary>
    /// A comma-separated list of URLs for schema registry instances
    /// </summary>
    public string Url { get; init; } = "http://localhost:8081";

    /// <summary>
    /// Maximum number of schemas to cache locally
    /// </summary>
    public int MaxCachedSchemas { get; init; } = 1000;

    /// <summary>
    /// Timeout for requests to Schema Registry (in milliseconds)
    /// </summary>
    public int RequestTimeoutMs { get; init; } = 30000;

    /// <summary>
    /// Basic auth credentials in the form {username}:{password}
    /// </summary>
    public string? BasicAuthUserInfo { get; init; }

    /// <summary>
    /// Basic auth credentials source
    /// </summary>
    public BasicAuthCredentialsSource BasicAuthCredentialsSource { get; init; } = BasicAuthCredentialsSource.UserInfo;

    /// <summary>
    /// Auto register schemas if not found
    /// </summary>
    public bool AutoRegisterSchemas { get; init; } = true;

    /// <summary>
    /// TTL for latest schema caches, or -1 for no TTL
    /// </summary>
    public int LatestCacheTtlSecs { get; init; } = 300;

    // SSL/TLS関連（Confluent標準）
    /// <summary>
    /// File path to CA certificate(s) for verifying the Schema Registry's key
    /// </summary>
    public string? SslCaLocation { get; init; }

    /// <summary>
    /// SSL keystore (PKCS#12) location
    /// </summary>
    public string? SslKeystoreLocation { get; init; }

    /// <summary>
    /// SSL keystore (PKCS#12) password
    /// </summary>
    public string? SslKeystorePassword { get; init; }

    /// <summary>
    /// SSL key password
    /// </summary>
    public string? SslKeyPassword { get; init; }

    /// <summary>
    /// 追加設定プロパティ（Confluent非標準プロパティ用）
    /// </summary>
    public Dictionary<string, string> AdditionalProperties { get; init; } = new();
}

/// <summary>
/// Basic auth credentials source (Confluent標準)
/// </summary>
public enum BasicAuthCredentialsSource
{
    /// <summary>
    /// Credentials via schema.registry.basic.auth.user.info
    /// </summary>
    UserInfo,

    /// <summary>
    /// Credentials via sasl.username and sasl.password
    /// </summary>
    SaslInherit
}