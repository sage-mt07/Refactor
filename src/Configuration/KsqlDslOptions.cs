using KsqlDsl.Messaging.Configuration;
using System.Collections.Generic;

namespace KsqlDsl.Configuration
{
    public class KsqlDslOptions
    {
        /// <summary>
        /// バリデーションモード（ルートレベル配置）
        /// </summary>
        public ValidationMode ValidationMode { get; init; } = ValidationMode.Strict;

        /// <summary>
        /// 共通設定（BootstrapServers, ClientId等）
        /// </summary>
        public CommonSection Common { get; init; } = new();

        /// <summary>
        /// トピック別設定（Producer/Consumer設定をトピック単位で管理）
        /// </summary>
        public Dictionary<string, TopicSection> Topics { get; init; } = new();

        /// <summary>
        /// Schema Registry設定
        /// </summary>
        public SchemaRegistrySection SchemaRegistry { get; init; } = new();
    }
}
