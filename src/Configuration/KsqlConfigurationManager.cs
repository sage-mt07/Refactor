// =============================================================================
// src/Configuration/KsqlConfigurationManager.cs
// =============================================================================
namespace KsqlDsl.Configuration;

using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Configuration.Overrides;
using KsqlDsl.Configuration.Validation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

/// <summary>
/// KsqlDsl統合設定管理実装
/// </summary>
public class KsqlConfigurationManager : IKsqlConfigurationManager
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<KsqlConfigurationManager> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Dictionary<Type, object> _optionsCache = new();
    private readonly Dictionary<Type, object> _validators = new();
    private readonly List<IConfigurationOverrideSource> _overrideSources = new();

    public event EventHandler<ConfigurationChangedEventArgs>? ConfigurationChanged;

    public KsqlConfigurationManager(
        IConfiguration configuration,
        ILoggerFactory loggerFactory)
    {
        _configuration = configuration;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<KsqlConfigurationManager>();

        RegisterDefaultOverrideSources();
        StartWatchingOverrideSources();
    }

    public T GetOptions<T>() where T : class, new()
    {
        if (_optionsCache.TryGetValue(typeof(T), out var cached))
        {
            return (T)cached;
        }

        var options = BuildOptions<T>();
        _optionsCache[typeof(T)] = options;

        return options;
    }

    public async Task ReloadAsync()
    {
        _logger.LogInformation("Reloading configuration...");

        _optionsCache.Clear();

        // 非同期でキャッシュを再構築
        await Task.Run(() =>
        {
            var types = new[]
            {
                typeof(KafkaBusOptions),
                typeof(KafkaProducerOptions),
                typeof(KafkaConsumerOptions),
                typeof(RetryOptions),
                typeof(AvroSchemaRegistryOptions)
            };

            foreach (var type in types)
            {
                var method = typeof(KsqlConfigurationManager).GetMethod(nameof(GetOptions))!.MakeGenericMethod(type);
                method.Invoke(this, null);
            }
        });

        _logger.LogInformation("Configuration reloaded successfully");
    }

    public ValidationResult ValidateAll()
    {
        var allErrors = new List<string>();
        var allWarnings = new List<string>();

        // すべてのオプション型を取得
        var optionTypes = new[]
        {
            typeof(KafkaBusOptions),
            typeof(KafkaProducerOptions),
            typeof(KafkaConsumerOptions),
            typeof(RetryOptions),
            typeof(AvroSchemaRegistryOptions)
        };

        foreach (var type in optionTypes)
        {
            try
            {
                // オプションを取得（キャッシュから、またはビルド）
                var options = GetOptionsByType(type);

                // バリデーターを取得して実行
                var validator = GetValidatorForType(type);
                if (validator != null)
                {
                    var result = validator.Validate(options);
                    allErrors.AddRange(result.Errors.Select(e => $"{type.Name}: {e}"));
                    allWarnings.AddRange(result.Warnings.Select(w => $"{type.Name}: {w}"));
                }
            }
            catch (Exception ex)
            {
                allErrors.Add($"{type.Name}: Failed to validate - {ex.Message}");
            }
        }

        return new ValidationResult
        {
            IsValid = allErrors.Count == 0,
            Errors = allErrors,
            Warnings = allWarnings
        };
    }

    private object GetOptionsByType(Type type)
    {
        if (_optionsCache.TryGetValue(type, out var cached))
        {
            return cached;
        }

        // リフレクションを使用してGetOptions<T>を呼び出し
        var method = typeof(KsqlConfigurationManager).GetMethod(nameof(GetOptions))!.MakeGenericMethod(type);
        return method.Invoke(this, null)!;
    }

    private dynamic? GetValidatorForType(Type type)
    {
        if (_validators.TryGetValue(type, out var validator))
        {
            return validator;
        }

        // デフォルトバリデーターを作成
        var validatorType = typeof(DefaultOptionValidator<>).MakeGenericType(type);

        try
        {
            // ILoggerFactoryから適切な型のロガーを作成
            var logger = _loggerFactory.CreateLogger(validatorType);
            var validatorInstance = Activator.CreateInstance(validatorType, logger);

            if (validatorInstance != null)
            {
                _validators[type] = validatorInstance;
                return validatorInstance;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create validator for type {Type}", type.Name);
        }

        return null;
    }

    private T BuildOptions<T>() where T : class, new()
    {
        var options = new T();
        var sectionName = GetSectionName<T>();

        // 基本設定をバインド
        _configuration.GetSection(sectionName).Bind(options);

        // 上書きソースを適用
        ApplyOverrides(options, sectionName);

        // バリデーション実行
        ValidateOptions(options, typeof(T));

        return options;
    }

    private void ValidateOptions<T>(T options, Type type) where T : class
    {
        var validator = GetValidatorForType(type);
        if (validator != null)
        {
            try
            {
                // dynamic経由でValidateAndThrowを呼び出し
                validator.ValidateAndThrow(options);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Validation failed for {OptionsType}", type.Name);
                throw;
            }
        }
    }

    private void ApplyOverrides<T>(T options, string sectionName) where T : class
    {
        var sortedSources = _overrideSources.OrderBy(s => s.Priority);

        foreach (var source in sortedSources)
        {
            var overrides = source.GetValues(sectionName);
            foreach (var (key, value) in overrides)
            {
                SetPropertyValue(options, key, value);
            }
        }
    }

    private void SetPropertyValue<T>(T options, string propertyPath, string value)
    {
        var properties = propertyPath.Split('.');
        object current = options!;

        for (int i = 0; i < properties.Length - 1; i++)
        {
            var prop = current.GetType().GetProperty(properties[i]);
            if (prop != null)
            {
                current = prop.GetValue(current)!;
            }
        }

        var finalProp = current.GetType().GetProperty(properties.Last());
        if (finalProp != null && finalProp.CanWrite)
        {
            var convertedValue = Convert.ChangeType(value, finalProp.PropertyType);
            finalProp.SetValue(current, convertedValue);
        }
    }

    private string GetSectionName<T>()
    {
        return typeof(T).Name.Replace("Options", "");
    }

    private void RegisterDefaultOverrideSources()
    {
        var envProvider = new EnvironmentOverrideProvider(
            _loggerFactory.CreateLogger<EnvironmentOverrideProvider>());
        _overrideSources.Add(envProvider);
    }

    private void StartWatchingOverrideSources()
    {
        foreach (var source in _overrideSources)
        {
            source.StartWatching(OnConfigurationOverrideChanged);
        }
    }

    private void OnConfigurationOverrideChanged(string key, string? value)
    {
        _logger.LogInformation("Configuration override changed: {Key} = {Value}", key, value);

        // 関連するオプションをキャッシュからクリア
        _optionsCache.Clear();

        ConfigurationChanged?.Invoke(this, new ConfigurationChangedEventArgs(key, value ?? string.Empty));
    }
}