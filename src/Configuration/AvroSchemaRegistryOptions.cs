using System;
using System.Collections.Generic;

namespace KsqlDsl.Configuration;



/// <summary>
/// Avroスキーマレジストリ構成
/// </summary>
public record AvroSchemaRegistryOptions
{
    public string Url { get; init; } = "http://localhost:8081";
    public int MaxCachedSchemas { get; init; } = 1000;
    public TimeSpan CacheExpirationTime { get; init; } = TimeSpan.FromHours(1);
    public string? BasicAuthUsername { get; init; }
    public string? BasicAuthPassword { get; init; }
    public List<string> BasicAuthCredentialsSource { get; init; } = new() { "UserInfo" };
    public int RequestTimeoutMs { get; init; } = 30000;
    public bool AutoRegisterSchemas { get; init; } = true;
    public SubjectNameStrategy SubjectNameStrategy { get; init; } = SubjectNameStrategy.Topic;
    public bool UseLatestVersion { get; init; } = false;
}

public enum SubjectNameStrategy
{
    Topic,
    Record,
    TopicRecord
}
