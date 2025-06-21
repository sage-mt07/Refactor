using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;

namespace KsqlDsl.Core.Extensions
{
    /// <summary>
    /// ILoggerFactory の汎用化拡張メソッド - Option 2完全版
    /// 設計理由: KafkaContext依存を排除し、Core層として適切なレイヤー設計を実現
    /// </summary>
    public static class LoggerFactoryExtensions
    {
        /// <summary>
        /// ILoggerFactory から型安全なロガーを作成、nullの場合はNullLoggerを返す
        /// </summary>
        /// <typeparam name="T">ロガーのカテゴリ型</typeparam>
        /// <param name="loggerFactory">ロガーファクトリ（null許可）</param>
        /// <returns>ILogger&lt;T&gt; または NullLogger&lt;T&gt;.Instance</returns>
        public static ILogger<T> CreateLoggerOrNull<T>(this ILoggerFactory? loggerFactory)
        {
            return loggerFactory?.CreateLogger<T>() ?? NullLogger<T>.Instance;
        }

        /// <summary>
        /// カテゴリ名指定版のロガー作成
        /// </summary>
        /// <param name="loggerFactory">ロガーファクトリ（null許可）</param>
        /// <param name="categoryName">カテゴリ名</param>
        /// <returns>ILogger または NullLogger.Instance</returns>
        public static ILogger CreateLoggerOrNull(this ILoggerFactory? loggerFactory, string categoryName)
        {
            return loggerFactory?.CreateLogger(categoryName) ?? NullLogger.Instance;
        }

        /// <summary>
        /// Type指定版のロガー作成
        /// </summary>
        /// <param name="loggerFactory">ロガーファクトリ（null許可）</param>
        /// <param name="type">カテゴリのType</param>
        /// <returns>ILogger または NullLogger.Instance</returns>
        public static ILogger CreateLoggerOrNull(this ILoggerFactory? loggerFactory, Type type)
        {
            return loggerFactory?.CreateLogger(type) ?? NullLogger.Instance;
        }

        /// <summary>
        /// 後方互換性を考慮したデバッグログ出力 - 汎用版
        /// </summary>
        /// <typeparam name="T">ロガーのカテゴリ型</typeparam>
        /// <param name="logger">ロガー</param>
        /// <param name="loggerFactory">ロガーファクトリ（null許可）</param>
        /// <param name="enableLegacyLogging">レガシーログ有効フラグ</param>
        /// <param name="message">メッセージテンプレート</param>
        /// <param name="args">引数</param>
        public static void LogDebugWithLegacySupport<T>(this ILogger<T> logger,
            ILoggerFactory? loggerFactory, bool enableLegacyLogging,
            string message, params object[] args)
        {
            // 新しいLoggerFactoryが設定されている場合
            if (loggerFactory != null)
            {
                logger.LogDebug(message, args);
            }
            // 後方互換性: 既存のEnableDebugLoggingフラグ
            else if (enableLegacyLogging)
            {
                Console.WriteLine($"[DEBUG] {string.Format(message, args)}");
            }
        }

        /// <summary>
        /// 後方互換性を考慮したデバッグログ出力 - 非ジェネリック版
        /// </summary>
        /// <param name="logger">ロガー</param>
        /// <param name="loggerFactory">ロガーファクトリ（null許可）</param>
        /// <param name="enableLegacyLogging">レガシーログ有効フラグ</param>
        /// <param name="message">メッセージテンプレート</param>
        /// <param name="args">引数</param>
        public static void LogDebugWithLegacySupport(this ILogger logger,
            ILoggerFactory? loggerFactory, bool enableLegacyLogging,
            string message, params object[] args)
        {
            // 新しいLoggerFactoryが設定されている場合
            if (loggerFactory != null)
            {
                logger.LogDebug(message, args);
            }
            // 後方互換性: 既存のEnableDebugLoggingフラグ
            else if (enableLegacyLogging)
            {
                Console.WriteLine($"[DEBUG] {string.Format(message, args)}");
            }
        }

        /// <summary>
        /// 後方互換性を考慮したInfoログ出力 - 汎用版
        /// </summary>
        /// <typeparam name="T">ロガーのカテゴリ型</typeparam>
        /// <param name="logger">ロガー</param>
        /// <param name="loggerFactory">ロガーファクトリ（null許可）</param>
        /// <param name="enableLegacyLogging">レガシーログ有効フラグ</param>
        /// <param name="message">メッセージテンプレート</param>
        /// <param name="args">引数</param>
        public static void LogInformationWithLegacySupport<T>(this ILogger<T> logger,
            ILoggerFactory? loggerFactory, bool enableLegacyLogging,
            string message, params object[] args)
        {
            if (loggerFactory != null)
            {
                logger.LogInformation(message, args);
            }
            else if (enableLegacyLogging)
            {
                Console.WriteLine($"[INFO] {string.Format(message, args)}");
            }
        }

        /// <summary>
        /// 後方互換性を考慮したWarningログ出力 - 汎用版
        /// </summary>
        /// <typeparam name="T">ロガーのカテゴリ型</typeparam>
        /// <param name="logger">ロガー</param>
        /// <param name="loggerFactory">ロガーファクトリ（null許可）</param>
        /// <param name="enableLegacyLogging">レガシーログ有効フラグ</param>
        /// <param name="message">メッセージテンプレート</param>
        /// <param name="args">引数</param>
        public static void LogWarningWithLegacySupport<T>(this ILogger<T> logger,
            ILoggerFactory? loggerFactory, bool enableLegacyLogging,
            string message, params object[] args)
        {
            if (loggerFactory != null)
            {
                logger.LogWarning(message, args);
            }
            else if (enableLegacyLogging)
            {
                Console.WriteLine($"[WARNING] {string.Format(message, args)}");
            }
        }

        /// <summary>
        /// 後方互換性を考慮したErrorログ出力 - 汎用版
        /// </summary>
        /// <typeparam name="T">ロガーのカテゴリ型</typeparam>
        /// <param name="logger">ロガー</param>
        /// <param name="exception">例外</param>
        /// <param name="loggerFactory">ロガーファクトリ（null許可）</param>
        /// <param name="enableLegacyLogging">レガシーログ有効フラグ</param>
        /// <param name="message">メッセージテンプレート</param>
        /// <param name="args">引数</param>
        public static void LogErrorWithLegacySupport<T>(this ILogger<T> logger,
            Exception exception, ILoggerFactory? loggerFactory, bool enableLegacyLogging,
            string message, params object[] args)
        {
            if (loggerFactory != null)
            {
                logger.LogError(exception, message, args);
            }
            else if (enableLegacyLogging)
            {
                Console.WriteLine($"[ERROR] {string.Format(message, args)}");
                Console.WriteLine($"[ERROR] Exception: {exception.Message}");
            }
        }
    }
}