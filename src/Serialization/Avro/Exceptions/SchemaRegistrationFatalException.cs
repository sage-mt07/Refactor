using System;

namespace KsqlDsl.Serialization.Avro.Exceptions
{
    /// <summary>
    /// Fail-Fast設計用のスキーマ登録致命的例外
    /// 人間の介入が必要な状態を示し、アプリケーションの即座終了を要求
    /// 設計方針：決して継続処理せず、運用者の介入を必須とする
    /// </summary>
    public class SchemaRegistrationFatalException : Exception
    {
        /// <summary>
        /// 失敗したスキーマのSubject名
        /// </summary>
        public string Subject { get; }

        /// <summary>
        /// リトライ試行回数
        /// </summary>
        public int AttemptCount { get; }

        /// <summary>
        /// 失敗時刻（運用追跡用）
        /// </summary>
        public DateTime FailedAt { get; }

        /// <summary>
        /// 失敗カテゴリ（運用分類用）
        /// </summary>
        public SchemaRegistrationFailureCategory FailureCategory { get; }

        /// <summary>
        /// 運用者向けアクション（対処法）
        /// </summary>
        public string OperationalAction { get; }

        /// <summary>
        /// コンストラクタ：リトライ失敗による致命的エラー
        /// </summary>
        /// <param name="subject">失敗したSubject名</param>
        /// <param name="attemptCount">リトライ試行回数</param>
        /// <param name="failureCategory">失敗カテゴリ</param>
        /// <param name="message">エラーメッセージ</param>
        /// <param name="innerException">元例外</param>
        public SchemaRegistrationFatalException(
            string subject,
            int attemptCount,
            SchemaRegistrationFailureCategory failureCategory,
            string message,
            Exception innerException)
            : base(FormatFatalMessage(subject, attemptCount, failureCategory, message), innerException)
        {
            Subject = subject ?? throw new ArgumentNullException(nameof(subject));
            AttemptCount = attemptCount;
            FailedAt = DateTime.UtcNow;
            FailureCategory = failureCategory;
            OperationalAction = DetermineOperationalAction(failureCategory);
        }

        /// <summary>
        /// コンストラクタ：即座失敗による致命的エラー（非リトライ例外）
        /// </summary>
        /// <param name="subject">失敗したSubject名</param>
        /// <param name="failureCategory">失敗カテゴリ</param>
        /// <param name="message">エラーメッセージ</param>
        /// <param name="innerException">元例外</param>
        public SchemaRegistrationFatalException(
            string subject,
            SchemaRegistrationFailureCategory failureCategory,
            string message,
            Exception innerException)
            : this(subject, 1, failureCategory, message, innerException)
        {
        }

        /// <summary>
        /// 運用者向けの詳細エラー情報取得
        /// </summary>
        public string GetOperationalSummary()
        {
            return $@"
🚨 SCHEMA REGISTRATION FATAL ERROR 🚨
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💀 HUMAN INTERVENTION REQUIRED - APPLICATION MUST BE TERMINATED 💀

📋 Error Details:
   Subject: {Subject}
   Failed At: {FailedAt:yyyy-MM-dd HH:mm:ss} UTC
   Attempts: {AttemptCount}
   Category: {FailureCategory}

🔧 Required Action:
   {OperationalAction}

⚠️ Root Cause:
   {InnerException?.GetType().Name ?? "Unknown"}: {InnerException?.Message ?? Message}

🚨 CRITICAL: Do not restart application until issue is resolved 🚨
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";
        }

        /// <summary>
        /// 致命的エラーメッセージの生成
        /// </summary>
        private static string FormatFatalMessage(
            string subject,
            int attemptCount,
            SchemaRegistrationFailureCategory category,
            string message)
        {
            var retryInfo = attemptCount > 1 ? $" after {attemptCount} attempts" : "";
            return $"💀 FATAL: Schema registration failed permanently for '{subject}'{retryInfo}. " +
                   $"Category: {category}. {message} 🚨 HUMAN INTERVENTION REQUIRED";
        }

        /// <summary>
        /// 失敗カテゴリに基づく運用アクション決定
        /// </summary>
        private static string DetermineOperationalAction(SchemaRegistrationFailureCategory category)
        {
            return category switch
            {
                SchemaRegistrationFailureCategory.NetworkFailure =>
                    "1. Check Schema Registry connectivity\n" +
                    "   2. Verify network configuration\n" +
                    "   3. Check firewall settings",

                SchemaRegistrationFailureCategory.AuthenticationFailure =>
                    "1. Verify Schema Registry credentials\n" +
                    "   2. Check API key validity\n" +
                    "   3. Confirm access permissions",

                SchemaRegistrationFailureCategory.SchemaIncompatible =>
                    "1. Review schema compatibility settings\n" +
                    "   2. Check schema evolution rules\n" +
                    "   3. Verify schema structure",

                SchemaRegistrationFailureCategory.RegistryUnavailable =>
                    "1. Check Schema Registry service status\n" +
                    "   2. Verify Registry health endpoints\n" +
                    "   3. Check cluster availability",

                SchemaRegistrationFailureCategory.ConfigurationError =>
                    "1. Review application configuration\n" +
                    "   2. Verify Schema Registry URL\n" +
                    "   3. Check client settings",

                SchemaRegistrationFailureCategory.ResourceExhausted =>
                    "1. Check Schema Registry disk space\n" +
                    "   2. Verify memory usage\n" +
                    "   3. Review rate limiting",

                _ =>
                    "1. Check application logs for details\n" +
                    "   2. Review Schema Registry logs\n" +
                    "   3. Contact system administrator"
            };
        }

        /// <summary>
        /// 例外の文字列表現（運用監視向け）
        /// </summary>
        public override string ToString()
        {
            return GetOperationalSummary();
        }
    }

    /// <summary>
    /// スキーマ登録失敗カテゴリ（運用分類用）
    /// </summary>
    public enum SchemaRegistrationFailureCategory
    {
        /// <summary>
        /// ネットワーク接続失敗
        /// </summary>
        NetworkFailure,

        /// <summary>
        /// 認証・認可失敗
        /// </summary>
        AuthenticationFailure,

        /// <summary>
        /// スキーマ互換性エラー
        /// </summary>
        SchemaIncompatible,

        /// <summary>
        /// Schema Registry利用不可
        /// </summary>
        RegistryUnavailable,

        /// <summary>
        /// 設定エラー
        /// </summary>
        ConfigurationError,

        /// <summary>
        /// リソース不足
        /// </summary>
        ResourceExhausted,

        /// <summary>
        /// 不明なエラー
        /// </summary>
        Unknown
    }
}