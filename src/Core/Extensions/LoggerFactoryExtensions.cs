using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
namespace KsqlDsl.Core.Extensions;

/// <summary>
/// ILoggerFactory の汎用化拡張メソッド
/// 設計理由: null チェックとNullLogger生成の定型文を排除
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
}

/// <summary>
/// KafkaContext向けのロガー拡張メソッド
/// </summary>
public static class KafkaContextLoggerExtensions
{
    /// <summary>
    /// KafkaContext から LoggerFactory を取得して型安全なロガーを作成
    /// </summary>
    /// <typeparam name="T">ロガーのカテゴリ型</typeparam>
    /// <param name="context">Kafkaコンテキスト</param>
    /// <returns>ILogger&lt;T&gt; または NullLogger&lt;T&gt;.Instance</returns>
    public static ILogger<T> CreateLogger<T>(this KafkaContext context)
    {
        return context.Options?.LoggerFactory.CreateLoggerOrNull<T>() ?? NullLogger<T>.Instance;
    }

    /// <summary>
    /// 後方互換性を考慮したデバッグログ出力
    /// </summary>
    /// <param name="logger">ロガー</param>
    /// <param name="context">Kafkaコンテキスト</param>
    /// <param name="message">メッセージ</param>
    /// <param name="args">引数</param>
    public static void LogDebugWithLegacySupport(this ILogger logger, KafkaContext context, string message, params object[] args)
    {
        // 新しいLoggerFactoryが設定されている場合
        if (context.Options.LoggerFactory != null)
        {
            logger.LogDebug(message, args);
        }
        // 後方互換性: 既存のEnableDebugLoggingフラグ
        else if (context.Options.EnableDebugLogging)
        {
            Console.WriteLine($"[DEBUG] {string.Format(message, args)}");
        }
    }
}