using System.Collections.Generic;

namespace KsqlDsl.Messaging.Configuration
{
    public class ProducerSection
    {
        /// <summary>
        /// 確認応答レベル（All, Leader, None）
        /// </summary>
        public string Acks { get; init; } = "All";

        /// <summary>
        /// 圧縮タイプ
        /// </summary>
        public string CompressionType { get; init; } = "Snappy";

        /// <summary>
        /// 冪等性有効化
        /// </summary>
        public bool EnableIdempotence { get; init; } = true;

        /// <summary>
        /// 最大インフライトリクエスト数
        /// </summary>
        public int MaxInFlightRequestsPerConnection { get; init; } = 1;

        /// <summary>
        /// バッチ送信前の待機時間（ミリ秒）
        /// </summary>
        public int LingerMs { get; init; } = 5;

        /// <summary>
        /// バッチサイズ（バイト）
        /// </summary>
        public int BatchSize { get; init; } = 16384;

        /// <summary>
        /// 配信タイムアウト（ミリ秒）
        /// </summary>
        public int DeliveryTimeoutMs { get; init; } = 120000;

        /// <summary>
        /// リトライバックオフ時間（ミリ秒）
        /// </summary>
        public int RetryBackoffMs { get; init; } = 100;

        /// <summary>
        /// 最大リトライ回数
        /// </summary>
        public int Retries { get; init; } = int.MaxValue;

        /// <summary>
        /// バッファメモリ（バイト）
        /// </summary>
        public long BufferMemory { get; init; } = 33554432; // 32MB

        /// <summary>
        /// パーティショナー
        /// </summary>
        public string? Partitioner { get; init; }

        /// <summary>
        /// 追加設定プロパティ（運用上の柔軟性確保）
        /// </summary>
        public Dictionary<string, string> AdditionalProperties { get; init; } = new();
    }
}
