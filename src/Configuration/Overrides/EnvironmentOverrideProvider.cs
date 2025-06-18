namespace KsqlDsl.Configuration.Overrides;

using Microsoft.Extensions.Logging;
using System;
using System.Collections;
using System.Collections.Generic;

/// <summary>
/// 環境変数による設定上書きプロバイダー
/// KafkaBus__BootstrapServers 形式を解釈
/// </summary>
public class EnvironmentOverrideProvider : IConfigurationOverrideSource
{
    private readonly ILogger<EnvironmentOverrideProvider> _logger;
    private readonly string _prefix;
    private readonly char _delimiter;

    public int Priority => 1; // 高優先度

    public EnvironmentOverrideProvider(
        ILogger<EnvironmentOverrideProvider> logger,
        string prefix = "KafkaBus",
        char delimiter = '_')
    {
        _logger = logger;
        _prefix = prefix;
        _delimiter = delimiter;
    }

    public string? GetValue(string key)
    {
        var envKey = ConvertToEnvironmentKey(key);
        var value = Environment.GetEnvironmentVariable(envKey);

        if (value != null)
        {
            _logger.LogDebug("Environment override found: {Key} = {Value}", envKey, value);
        }

        return value;
    }

    public Dictionary<string, string> GetValues(string prefix)
    {
        var result = new Dictionary<string, string>();
        var envPrefix = $"{_prefix}{_delimiter}{prefix}";

        foreach (DictionaryEntry env in Environment.GetEnvironmentVariables())
        {
            var key = env.Key.ToString()!;
            if (key.StartsWith(envPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var configKey = ConvertFromEnvironmentKey(key);
                result[configKey] = env.Value!.ToString()!;
            }
        }

        return result;
    }

    public void StartWatching(Action<string, string?> onChanged)
    {
        // 環境変数は通常実行時に変更されないため、監視は実装しない
        _logger.LogInformation("Environment variable watching is not supported");
    }

    public void StopWatching()
    {
        // No-op
    }

    private string ConvertToEnvironmentKey(string configKey)
    {
        // "Producer.Acks" -> "KafkaBus__Producer__Acks"
        return $"{_prefix}{_delimiter}{_delimiter}{configKey.Replace('.', _delimiter)}";
    }

    private string ConvertFromEnvironmentKey(string envKey)
    {
        // "KafkaBus__Producer__Acks" -> "Producer.Acks"
        var prefix = $"{_prefix}{_delimiter}{_delimiter}";
        return envKey.Substring(prefix.Length).Replace(_delimiter, '.');
    }
}