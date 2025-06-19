using Confluent.SchemaRegistry;
using KsqlDsl.Configuration.Options;
using KsqlDsl.Monitoring.Metrics;
using KsqlDsl.Monitoring.Tracing;
using KsqlDsl.Serialization.Avro.Cache;
using KsqlDsl.Serialization.Avro.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro
{
    public class ResilientAvroSerializerManager
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly AvroOperationRetrySettings _retrySettings;
        private readonly ILogger<ResilientAvroSerializerManager> _logger;
        private readonly AvroMetricsCollector? _metricsCollector;

        public ResilientAvroSerializerManager(
            ISchemaRegistryClient schemaRegistryClient,
            IOptions<AvroOperationRetrySettings> retrySettings,
            ILogger<ResilientAvroSerializerManager> logger,
            AvroMetricsCollector? metricsCollector = null)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _retrySettings = retrySettings?.Value ?? throw new ArgumentNullException(nameof(retrySettings));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _metricsCollector = metricsCollector;
        }

        public async Task<int> RegisterSchemaWithRetryAsync(string subject, string schema)
        {
            using var activity = AvroActivitySource.StartSchemaRegistration(subject);
            var policy = _retrySettings.SchemaRegistration;
            var attempt = 1;

            while (attempt <= policy.MaxAttempts)
            {
                try
                {
                    using var operation = AvroActivitySource.StartCacheOperation("register", subject);
                    var stopwatch = Stopwatch.StartNew();
                    var schemaObj = new Schema(schema, SchemaType.Avro);
                    var schemaId = await _schemaRegistryClient.RegisterSchemaAsync(subject, schemaObj);
                    stopwatch.Stop();

                    _logger.LogInformation(
                        "Schema registration succeeded: {Subject} (ID: {SchemaId}, Attempt: {Attempt}, Duration: {Duration}ms)",
                        subject, schemaId, attempt, stopwatch.ElapsedMilliseconds);

                    _metricsCollector?.RecordSchemaRegistration(subject, true, stopwatch.Elapsed);

                    activity?.SetTag("schema.id", schemaId)?.SetStatus(ActivityStatusCode.Ok);
                    return schemaId;
                }
                catch (Exception ex) when (ShouldRetry(ex, policy, attempt))
                {
                    var delay = CalculateDelay(policy, attempt);

                    _logger.LogWarning(ex,
                        "Schema registration retry: {Subject} (Attempt: {Attempt}/{MaxAttempts}, Delay: {Delay}ms)",
                        subject, attempt, policy.MaxAttempts, delay.TotalMilliseconds);

                    await Task.Delay(delay);
                    attempt++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Schema registration failed permanently: {Subject} (Attempts: {Attempts})",
                        subject, attempt);

                    _metricsCollector?.RecordSchemaRegistration(subject, false, TimeSpan.Zero);

                    activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                    throw;
                }
            }

            throw new InvalidOperationException($"Schema registration failed after {policy.MaxAttempts} attempts: {subject}");
        }

        public async Task<AvroSchemaInfo> GetSchemaWithRetryAsync(string subject, int version)
        {
            var policy = _retrySettings.SchemaRetrieval;
            var attempt = 1;

            while (attempt <= policy.MaxAttempts)
            {
                try
                {
                    var stopwatch = Stopwatch.StartNew();
                    // 新しいAPIを使用
                    var registeredSchema = await _schemaRegistryClient.GetRegisteredSchemaAsync(subject, version);
                    stopwatch.Stop();

                    _logger.LogDebug(
                        "Schema retrieval succeeded: {Subject} v{Version} (Attempt: {Attempt}, Duration: {Duration}ms)",
                        subject, version, attempt, stopwatch.ElapsedMilliseconds);

                    return new AvroSchemaInfo
                    {
                        EntityType = typeof(object),
                        Type = SerializerType.Value,
                        SchemaId = registeredSchema.Id,
                        Subject = registeredSchema.Subject,
                        RegisteredAt = DateTime.UtcNow,
                        LastUsed = DateTime.UtcNow,
                        AvroSchema = registeredSchema.SchemaString,
                        Version = registeredSchema.Version,
                        UsageCount = 0
                    };
                }
                catch (Exception ex) when (ShouldRetry(ex, policy, attempt))
                {
                    var delay = CalculateDelay(policy, attempt);

                    _logger.LogWarning(ex,
                        "Schema retrieval retry: {Subject} v{Version} (Attempt: {Attempt}/{MaxAttempts}, Delay: {Delay}ms)",
                        subject, version, attempt, policy.MaxAttempts, delay.TotalMilliseconds);

                    await Task.Delay(delay);
                    attempt++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Schema retrieval failed permanently: {Subject} v{Version} (Attempts: {Attempts})",
                        subject, version, attempt);
                    throw;
                }
            }

            throw new InvalidOperationException($"Schema retrieval failed after {policy.MaxAttempts} attempts: {subject} v{version}");
        }

        public async Task<bool> CheckCompatibilityWithRetryAsync(string subject, string schema)
        {
            var policy = _retrySettings.CompatibilityCheck;
            var attempt = 1;

            while (attempt <= policy.MaxAttempts)
            {
                try
                {
                    var stopwatch = Stopwatch.StartNew();
                    // Schemaオブジェクトを作成して互換性チェック
                    var schemaObj = new Schema(schema, SchemaType.Avro);
                    var isCompatible = await _schemaRegistryClient.IsCompatibleAsync(subject, schemaObj);
                    stopwatch.Stop();

                    _logger.LogDebug(
                        "Compatibility check succeeded: {Subject} (Attempt: {Attempt}, Duration: {Duration}ms, Result: {Result})",
                        subject, attempt, stopwatch.ElapsedMilliseconds, isCompatible);

                    return isCompatible;
                }
                catch (Exception ex) when (ShouldRetry(ex, policy, attempt))
                {
                    var delay = CalculateDelay(policy, attempt);

                    _logger.LogWarning(ex,
                        "Compatibility check retry: {Subject} (Attempt: {Attempt}/{MaxAttempts}, Delay: {Delay}ms)",
                        subject, attempt, policy.MaxAttempts, delay.TotalMilliseconds);

                    await Task.Delay(delay);
                    attempt++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Compatibility check failed permanently: {Subject} (Attempts: {Attempts})",
                        subject, attempt);
                    throw;
                }
            }

            throw new InvalidOperationException($"Compatibility check failed after {policy.MaxAttempts} attempts: {subject}");
        }

        private bool ShouldRetry(Exception ex, AvroRetryPolicy policy, int attempt)
        {
            if (attempt >= policy.MaxAttempts) return false;
            if (policy.NonRetryableExceptions.Any(type => type.IsInstanceOfType(ex))) return false;
            return policy.RetryableExceptions.Any(type => type.IsInstanceOfType(ex));
        }

        private TimeSpan CalculateDelay(AvroRetryPolicy policy, int attempt)
        {
            var delay = TimeSpan.FromMilliseconds(
                policy.InitialDelay.TotalMilliseconds * Math.Pow(policy.BackoffMultiplier, attempt - 1));
            return delay > policy.MaxDelay ? policy.MaxDelay : delay;
        }
    }
}