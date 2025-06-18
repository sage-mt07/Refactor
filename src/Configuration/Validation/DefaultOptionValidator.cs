namespace KsqlDsl.Configuration.Validation;

using KsqlDsl.Configuration.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

/// <summary>
/// デフォルトオプションバリデーター
/// </summary>
public class DefaultOptionValidator<T> : IOptionValidator<T> where T : class
{
    private readonly ILogger<DefaultOptionValidator<T>> _logger;
    private readonly List<Func<T, ValidationResult>> _validators = new();

    public DefaultOptionValidator(ILogger<DefaultOptionValidator<T>> logger)
    {
        _logger = logger;
        RegisterDefaultValidators();
    }

    public ValidationResult Validate(T options)
    {
        var allErrors = new List<string>();
        var allWarnings = new List<string>();

        foreach (var validator in _validators)
        {
            var result = validator(options);
            allErrors.AddRange(result.Errors);
            allWarnings.AddRange(result.Warnings);
        }

        // 警告をログ出力
        foreach (var warning in allWarnings)
        {
            _logger.LogWarning("Configuration warning for {OptionsType}: {Warning}",
                typeof(T).Name, warning);
        }

        return new ValidationResult
        {
            IsValid = allErrors.Count == 0,
            Errors = allErrors,
            Warnings = allWarnings
        };
    }

    public void ValidateAndThrow(T options)
    {
        var result = Validate(options);
        if (!result.IsValid)
        {
            throw new ConfigurationValidationException(
                $"Configuration validation failed for {typeof(T).Name}: {string.Join(", ", result.Errors)}");
        }
    }

    protected virtual void RegisterDefaultValidators()
    {
        // KafkaBusOptions専用バリデーション
        if (typeof(T) == typeof(KafkaBusOptions))
        {
            _validators.Add(options =>
            {
                var kafkaOptions = options as KafkaBusOptions;
                var errors = new List<string>();

                if (string.IsNullOrWhiteSpace(kafkaOptions!.BootstrapServers))
                    errors.Add("BootstrapServers cannot be empty");

                if (kafkaOptions.RequestTimeoutMs <= 0)
                    errors.Add("RequestTimeoutMs must be positive");

                return errors.Any() ? ValidationResult.Failure(errors.ToArray()) : ValidationResult.Success();
            });
        }

        // RetryOptions専用バリデーション
        if (typeof(T) == typeof(RetryOptions))
        {
            _validators.Add(options =>
            {
                var retryOptions = options as RetryOptions;
                var errors = new List<string>();

                if (retryOptions!.MaxRetryAttempts < 0)
                    errors.Add("MaxRetryAttempts cannot be negative");

                if (retryOptions.BackoffMultiplier <= 1.0)
                    errors.Add("BackoffMultiplier must be greater than 1.0");

                return errors.Any() ? ValidationResult.Failure(errors.ToArray()) : ValidationResult.Success();
            });
        }
    }

    public void AddValidator(Func<T, ValidationResult> validator)
    {
        _validators.Add(validator);
    }
}

public class ConfigurationValidationException : Exception
{
    public ConfigurationValidationException(string message) : base(message) { }
    public ConfigurationValidationException(string message, Exception innerException) : base(message, innerException) { }
}