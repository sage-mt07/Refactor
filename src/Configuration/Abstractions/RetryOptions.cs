using System;
using System.Collections.Generic;

namespace KsqlDsl.Configuration.Abstractions;


/// <summary>
/// リトライ構成設定
/// </summary>
public record RetryOptions
{
    public int MaxRetryAttempts { get; init; } = 3;
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);
    public double BackoffMultiplier { get; init; } = 2.0;
    public bool EnableJitter { get; init; } = true;
    public List<Type> RetriableExceptions { get; init; } = new()
    {
        typeof(TimeoutException),
        typeof(InvalidOperationException)
    };
    public List<Type> NonRetriableExceptions { get; init; } = new()
    {
        typeof(ArgumentException),
        typeof(UnauthorizedAccessException)
    };
}
