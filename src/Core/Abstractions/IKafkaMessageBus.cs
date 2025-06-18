using KsqlDsl.Configuration.Options;
using KsqlDsl.Core.Models;
using KsqlDsl.Monitoring.Diagnostics;
using KsqlDsl.Monitoring.Health;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions
{

    /// <summary>
    /// KafkaMessageBus統合インターフェース
    /// 設計理由：アプリケーション層への統一APIの提供
    /// </summary>
    public interface IKafkaMessageBus : IDisposable
    {
        /// <summary>
        /// 単一メッセージ送信
        /// </summary>
        Task SendAsync<T>(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default) where T : class;

        /// <summary>
        /// バッチメッセージ送信
        /// </summary>
        Task SendBatchAsync<T>(IEnumerable<T> messages, KafkaMessageContext? context = null, CancellationToken cancellationToken = default) where T : class;

        /// <summary>
        /// リアルタイム消費ストリーム
        /// </summary>
        IAsyncEnumerable<T> ConsumeAsync<T>(KafkaSubscriptionOptions? options = null, CancellationToken cancellationToken = default) where T : class;

        /// <summary>
        /// バッチフェッチ（Pull型取得）
        /// </summary>
        Task<List<T>> FetchAsync<T>(KafkaFetchOptions options, CancellationToken cancellationToken = default) where T : class;

        /// <summary>
        /// ヘルスレポート取得
        /// </summary>
        Task<KafkaHealthReport> GetHealthReportAsync();

        /// <summary>
        /// 診断情報取得
        /// </summary>
        KafkaDiagnostics GetDiagnostics();
    }

}
