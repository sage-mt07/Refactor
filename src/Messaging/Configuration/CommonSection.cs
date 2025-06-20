using Confluent.Kafka;
using System.Collections.Generic;

namespace KsqlDsl.Messaging.Configuration
{
    public class CommonSection
    {
        /// <summary>
        /// Kafkaブローカーのアドレス（カンマ区切り）
        /// </summary>
        public string BootstrapServers { get; init; } = "localhost:9092";

        /// <summary>
        /// クライアントID
        /// </summary>
        public string ClientId { get; init; } = "ksql-dsl-client";

        /// <summary>
        /// リクエストタイムアウト（ミリ秒）
        /// </summary>
        public int RequestTimeoutMs { get; init; } = 30000;

        /// <summary>
        /// メタデータ最大有効期間（ミリ秒）
        /// </summary>
        public int MetadataMaxAgeMs { get; init; } = 300000;

        /// <summary>
        /// セキュリティプロトコル
        /// </summary>
        public SecurityProtocol SecurityProtocol { get; init; } = SecurityProtocol.Plaintext;

        /// <summary>
        /// SASLメカニズム
        /// </summary>
        public SaslMechanism? SaslMechanism { get; init; }

        /// <summary>
        /// SASLユーザー名
        /// </summary>
        public string? SaslUsername { get; init; }

        /// <summary>
        /// SASLパスワード
        /// </summary>
        public string? SaslPassword { get; init; }

        /// <summary>
        /// SSL CA証明書の場所
        /// </summary>
        public string? SslCaLocation { get; init; }

        /// <summary>
        /// SSL証明書の場所
        /// </summary>
        public string? SslCertificateLocation { get; init; }

        /// <summary>
        /// SSLキーの場所
        /// </summary>
        public string? SslKeyLocation { get; init; }

        /// <summary>
        /// SSLキーパスワード
        /// </summary>
        public string? SslKeyPassword { get; init; }

        /// <summary>
        /// 追加設定プロパティ（運用上の柔軟性確保）
        /// </summary>
        public Dictionary<string, string> AdditionalProperties { get; init; } = new();
    }
}
