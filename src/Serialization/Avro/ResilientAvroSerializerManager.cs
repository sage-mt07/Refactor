// src/Serialization/Avro/ResilientAvroSerializerManager.cs
// Monitoring除去、詳細リトライログ対応、Fail Fast強化版

using Confluent.SchemaRegistry;
using KsqlDsl.Configuration.Options;
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

        public ResilientAvroSerializerManager(
            ISchemaRegistryClient schemaRegistryClient,
            IOptions<AvroOperationRetrySettings> retrySettings,
            ILogger<ResilientAvroSerializerManager> logger)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _retrySettings = retrySettings?.Value ?? throw new ArgumentNullException(nameof(retrySettings));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// スキーマ登録（初期化パス = Monitoring無効、詳細リトライログ有効）
        /// </summary>
        public async Task<int> RegisterSchemaWithRetryAsync(string subject, string schema)
        {
            // ❌ 削除: Tracing/Activity（初期化パスでは無効）
            // using var activity = AvroActivitySource.StartSchemaRegistration(subject);

            var policy = _retrySettings.SchemaRegistration;
            var attempt = 1;
            var totalStartTime = DateTime.UtcNow;

            _logger.LogInformation("Schema registration started: {Subject} (MaxAttempts: {MaxAttempts})",
                subject, policy.MaxAttempts);

            while (attempt <= policy.MaxAttempts)
            {
                var attemptStartTime = DateTime.UtcNow;

                try
                {
                    // ❌ 削除: Cache操作のTracing
                    // using var operation = AvroActivitySource.StartCacheOperation("register", subject);

                    var stopwatch = Stopwatch.StartNew();
                    var schemaObj = new Schema(schema, SchemaType.Avro);
                    var schemaId = await _schemaRegistryClient.RegisterSchemaAsync(subject, schemaObj);
                    stopwatch.Stop();

                    var totalDuration = DateTime.UtcNow - totalStartTime;

                    // 成功ログ：何回目で成功したかを明記
                    _logger.LogInformation(
                        "Schema registration SUCCESS on attempt {Attempt}/{MaxAttempts}: {Subject} " +
                        "(SchemaID: {SchemaId}, AttemptDuration: {AttemptDuration}ms, TotalDuration: {TotalDuration}ms)",
                        attempt, policy.MaxAttempts, subject, schemaId,
                        stopwatch.ElapsedMilliseconds, totalDuration.TotalMilliseconds);

                    return schemaId;
                }
                catch (Exception ex) when (ShouldRetry(ex, policy, attempt))
                {
                    var attemptDuration = DateTime.UtcNow - attemptStartTime;
                    var delay = CalculateDelay(policy, attempt);

                    // 警告ログ：例外種別・タイミング・詳細を必ず明記
                    _logger.LogWarning(ex,
                        "RETRY ATTEMPT {Attempt}/{MaxAttempts} [{ExceptionType}] at {Timestamp}: " +
                        "Schema registration failed for {Subject}. " +
                        "AttemptDuration: {AttemptDuration}ms, NextRetryIn: {DelayMs}ms. " +
                        "ExceptionMessage: {ExceptionMessage} | StackTrace: {StackTrace}",
                        attempt, policy.MaxAttempts, ex.GetType().Name, DateTime.UtcNow,
                        subject, attemptDuration.TotalMilliseconds, delay.TotalMilliseconds,
                        ex.Message, ex.StackTrace?.Split('\n').FirstOrDefault()?.Trim() ?? "N/A");

                    await Task.Delay(delay);
                    attempt++;
                }
                catch (Exception ex)
                {
                    var attemptDuration = DateTime.UtcNow - attemptStartTime;
                    var totalDuration = DateTime.UtcNow - totalStartTime;

                    // 致命的エラー：人間介入必要を明記、Fail Fast実行
                    _logger.LogCritical(ex,
                        "FATAL ERROR: Schema registration failed permanently after {Attempt}/{MaxAttempts} attempts for {Subject}. " +
                        "AttemptDuration: {AttemptDuration}ms, TotalDuration: {TotalDuration}ms. " +
                        "ExceptionType: {ExceptionType}, ExceptionMessage: {ExceptionMessage}. " +
                        "HUMAN INTERVENTION REQUIRED. Application will TERMINATE immediately (Fail Fast).",
                        attempt, policy.MaxAttempts, subject,
                        attemptDuration.TotalMilliseconds, totalDuration.TotalMilliseconds,
                        ex.GetType().Name, ex.Message);

                    throw; // 即例外throw = アプリ停止（Fail Fast）
                }
            }

            // 最大試行到達時の致命的エラー
            var finalTotalDuration = DateTime.UtcNow - totalStartTime;
            var fatalMessage =
                $"FATAL: Schema registration exhausted all {policy.MaxAttempts} attempts for {subject}. " +
                $"TotalDuration: {finalTotalDuration.TotalMilliseconds}ms. " +
                $"HUMAN INTERVENTION REQUIRED.";

            _logger.LogCritical(fatalMessage);
            throw new InvalidOperationException(fatalMessage);
        }

        /// <summary>
        /// スキーマ取得（初期化パス = Monitoring無効、詳細リトライログ有効）
        /// </summary>
        public async Task<AvroSchemaInfo> GetSchemaWithRetryAsync(string subject, int version)
        {
            var policy = _retrySettings.SchemaRetrieval;
            var attempt = 1;
            var totalStartTime = DateTime.UtcNow;

            _logger.LogDebug("Schema retrieval started: {Subject} v{Version} (MaxAttempts: {MaxAttempts})",
                subject, version, policy.MaxAttempts);

            while (attempt <= policy.MaxAttempts)
            {
                var attemptStartTime = DateTime.UtcNow;

                try
                {
                    var stopwatch = Stopwatch.StartNew();
                    var registeredSchema = await _schemaRegistryClient.GetRegisteredSchemaAsync(subject, version);
                    stopwatch.Stop();

                    var totalDuration = DateTime.UtcNow - totalStartTime;

                    // 成功ログ：何回目で成功したかを明記
                    _logger.LogDebug(
                        "Schema retrieval SUCCESS on attempt {Attempt}/{MaxAttempts}: {Subject} v{Version} " +
                        "(AttemptDuration: {AttemptDuration}ms, TotalDuration: {TotalDuration}ms)",
                        attempt, policy.MaxAttempts, subject, version,
                        stopwatch.ElapsedMilliseconds, totalDuration.TotalMilliseconds);

                    return new AvroSchemaInfo
                    {
                        EntityType = typeof(object),
                        TopicName = ExtractTopicFromSubject(subject), // subject から topic名抽出
                        KeySchemaId = version == -1 ? 0 : registeredSchema.Id,
                        ValueSchemaId = version != -1 ? registeredSchema.Id : 0,
                        KeySchema = version == -1 ? "" : registeredSchema.SchemaString,
                        ValueSchema = version != -1 ? registeredSchema.SchemaString : "",
                        RegisteredAt = DateTime.UtcNow,
                        KeyProperties = null
                    };
                }
                catch (Exception ex) when (ShouldRetry(ex, policy, attempt))
                {
                    var attemptDuration = DateTime.UtcNow - attemptStartTime;
                    var delay = CalculateDelay(policy, attempt);

                    // 警告ログ：例外種別・タイミング・詳細を必ず明記
                    _logger.LogWarning(ex,
                        "RETRY ATTEMPT {Attempt}/{MaxAttempts} [{ExceptionType}] at {Timestamp}: " +
                        "Schema retrieval failed for {Subject} v{Version}. " +
                        "AttemptDuration: {AttemptDuration}ms, NextRetryIn: {DelayMs}ms. " +
                        "ExceptionMessage: {ExceptionMessage}",
                        attempt, policy.MaxAttempts, ex.GetType().Name, DateTime.UtcNow,
                        subject, version, attemptDuration.TotalMilliseconds, delay.TotalMilliseconds,
                        ex.Message);

                    await Task.Delay(delay);
                    attempt++;
                }
                catch (Exception ex)
                {
                    var attemptDuration = DateTime.UtcNow - attemptStartTime;
                    var totalDuration = DateTime.UtcNow - totalStartTime;

                    // 致命的エラー：人間介入必要を明記、Fail Fast実行
                    _logger.LogCritical(ex,
                        "FATAL ERROR: Schema retrieval failed permanently after {Attempt}/{MaxAttempts} attempts for {Subject} v{Version}. " +
                        "AttemptDuration: {AttemptDuration}ms, TotalDuration: {TotalDuration}ms. " +
                        "ExceptionType: {ExceptionType}, ExceptionMessage: {ExceptionMessage}. " +
                        "HUMAN INTERVENTION REQUIRED. Application will TERMINATE immediately (Fail Fast).",
                        attempt, policy.MaxAttempts, subject, version,
                        attemptDuration.TotalMilliseconds, totalDuration.TotalMilliseconds,
                        ex.GetType().Name, ex.Message);

                    throw; // 即例外throw = アプリ停止（Fail Fast）
                }
            }

            // 最大試行到達時の致命的エラー
            var finalTotalDuration = DateTime.UtcNow - totalStartTime;
            var fatalMessage =
                $"FATAL: Schema retrieval exhausted all {policy.MaxAttempts} attempts for {subject} v{version}. " +
                $"TotalDuration: {finalTotalDuration.TotalMilliseconds}ms. " +
                $"HUMAN INTERVENTION REQUIRED.";

            _logger.LogCritical(fatalMessage);
            throw new InvalidOperationException(fatalMessage);
        }
        private string ExtractTopicFromSubject(string subject)
        {
            if (subject.EndsWith("-key"))
                return subject.Substring(0, subject.Length - 4);
            if (subject.EndsWith("-value"))
                return subject.Substring(0, subject.Length - 6);
            return subject;
        }
        /// <summary>
        /// 互換性チェック（初期化パス = Monitoring無効、詳細リトライログ有効）
        /// </summary>
        public async Task<bool> CheckCompatibilityWithRetryAsync(string subject, string schema)
        {
            var policy = _retrySettings.CompatibilityCheck;
            var attempt = 1;
            var totalStartTime = DateTime.UtcNow;

            _logger.LogDebug("Compatibility check started: {Subject} (MaxAttempts: {MaxAttempts})",
                subject, policy.MaxAttempts);

            while (attempt <= policy.MaxAttempts)
            {
                var attemptStartTime = DateTime.UtcNow;

                try
                {
                    var stopwatch = Stopwatch.StartNew();
                    var schemaObj = new Schema(schema, SchemaType.Avro);
                    var isCompatible = await _schemaRegistryClient.IsCompatibleAsync(subject, schemaObj);
                    stopwatch.Stop();

                    var totalDuration = DateTime.UtcNow - totalStartTime;

                    // 成功ログ：何回目で成功したかを明記
                    _logger.LogDebug(
                        "Compatibility check SUCCESS on attempt {Attempt}/{MaxAttempts}: {Subject} " +
                        "(Result: {Compatible}, AttemptDuration: {AttemptDuration}ms, TotalDuration: {TotalDuration}ms)",
                        attempt, policy.MaxAttempts, subject, isCompatible,
                        stopwatch.ElapsedMilliseconds, totalDuration.TotalMilliseconds);

                    return isCompatible;
                }
                catch (Exception ex) when (ShouldRetry(ex, policy, attempt))
                {
                    var attemptDuration = DateTime.UtcNow - attemptStartTime;
                    var delay = CalculateDelay(policy, attempt);

                    // 警告ログ：例外種別・タイミング・詳細を必ず明記
                    _logger.LogWarning(ex,
                        "RETRY ATTEMPT {Attempt}/{MaxAttempts} [{ExceptionType}] at {Timestamp}: " +
                        "Compatibility check failed for {Subject}. " +
                        "AttemptDuration: {AttemptDuration}ms, NextRetryIn: {DelayMs}ms. " +
                        "ExceptionMessage: {ExceptionMessage}",
                        attempt, policy.MaxAttempts, ex.GetType().Name, DateTime.UtcNow,
                        subject, attemptDuration.TotalMilliseconds, delay.TotalMilliseconds,
                        ex.Message);

                    await Task.Delay(delay);
                    attempt++;
                }
                catch (Exception ex)
                {
                    var attemptDuration = DateTime.UtcNow - attemptStartTime;
                    var totalDuration = DateTime.UtcNow - totalStartTime;

                    // 致命的エラー：人間介入必要を明記、Fail Fast実行
                    _logger.LogCritical(ex,
                        "FATAL ERROR: Compatibility check failed permanently after {Attempt}/{MaxAttempts} attempts for {Subject}. " +
                        "AttemptDuration: {AttemptDuration}ms, TotalDuration: {TotalDuration}ms. " +
                        "ExceptionType: {ExceptionType}, ExceptionMessage: {ExceptionMessage}. " +
                        "HUMAN INTERVENTION REQUIRED. Application will TERMINATE immediately (Fail Fast).",
                        attempt, policy.MaxAttempts, subject,
                        attemptDuration.TotalMilliseconds, totalDuration.TotalMilliseconds,
                        ex.GetType().Name, ex.Message);

                    throw; // 即例外throw = アプリ停止（Fail Fast）
                }
            }

            // 最大試行到達時の致命的エラー
            var finalTotalDuration = DateTime.UtcNow - totalStartTime;
            var fatalMessage =
                $"FATAL: Compatibility check exhausted all {policy.MaxAttempts} attempts for {subject}. " +
                $"TotalDuration: {finalTotalDuration.TotalMilliseconds}ms. " +
                $"HUMAN INTERVENTION REQUIRED.";

            _logger.LogCritical(fatalMessage);
            throw new InvalidOperationException(fatalMessage);
        }

        #region Private Methods

        private bool ShouldRetry(Exception ex, AvroRetryPolicy policy, int attempt)
        {
            if (attempt >= policy.MaxAttempts) return false;

            // Non-retryable例外チェック
            if (policy.NonRetryableExceptions.Any(type => type.IsInstanceOfType(ex)))
            {
                _logger.LogDebug("Non-retryable exception detected: {ExceptionType}", ex.GetType().Name);
                return false;
            }

            // Retryable例外チェック
            var isRetryable = policy.RetryableExceptions.Any(type => type.IsInstanceOfType(ex));
            _logger.LogDebug("Retry decision for {ExceptionType}: {IsRetryable}", ex.GetType().Name, isRetryable);

            return isRetryable;
        }

        private TimeSpan CalculateDelay(AvroRetryPolicy policy, int attempt)
        {
            var delay = TimeSpan.FromMilliseconds(
                policy.InitialDelay.TotalMilliseconds * Math.Pow(policy.BackoffMultiplier, attempt - 1));

            var finalDelay = delay > policy.MaxDelay ? policy.MaxDelay : delay;

            _logger.LogDebug("Calculated retry delay for attempt {Attempt}: {Delay}ms (max: {MaxDelay}ms)",
                attempt, finalDelay.TotalMilliseconds, policy.MaxDelay.TotalMilliseconds);

            return finalDelay;
        }

        #endregion
    }
}