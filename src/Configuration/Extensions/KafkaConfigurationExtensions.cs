using Confluent.Kafka;
using KsqlDsl.Messaging.Configuration;
using KsqlDsl.Monitoring.Abstractions.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace KsqlDsl.Configuration.Extensions
{

    /// <summary>
    /// Kafka設定拡張メソッド
    /// </summary>
    public static class KafkaConfigurationExtensions
    {
        /// <summary>
        /// Producer設定をConfluentProducerConfigに変換
        /// </summary>
        public static ProducerConfig ToConfluentConfig(this KafkaProducerConfig config)
        {
            var confluentConfig = new ProducerConfig
            {
                BootstrapServers = config.BootstrapServers,
                Acks = (Confluent.Kafka.Acks)config.Acks,
                EnableIdempotence = config.EnableIdempotence,
                MaxInFlight = config.MaxInFlight,
                CompressionType = (Confluent.Kafka.CompressionType)config.CompressionType,

                LingerMs = config.LingerMs,
                BatchSize = config.BatchSize,
                RequestTimeoutMs = config.RequestTimeoutMs,
             
                RetryBackoffMs = config.RetryBackoffMs,
                SecurityProtocol = (Confluent.Kafka.SecurityProtocol)config.SecurityProtocol
            };

            if (!string.IsNullOrEmpty(config.SaslMechanism))
            {
                confluentConfig.SaslMechanism = Enum.Parse<SaslMechanism>(config.SaslMechanism);
            }

            if (!string.IsNullOrEmpty(config.SaslUsername))
            {
                confluentConfig.SaslUsername = config.SaslUsername;
            }

            if (!string.IsNullOrEmpty(config.SaslPassword))
            {
                confluentConfig.SaslPassword = config.SaslPassword;
            }

            // 追加設定を適用
            foreach (var kvp in config.AdditionalConfig)
            {
                confluentConfig.Set(kvp.Key, kvp.Value?.ToString());
            }

            return confluentConfig;
        }

        /// <summary>
        /// Consumer設定をConfluentConsumerConfigに変換
        /// </summary>
        public static ConsumerConfig ToConfluentConfig(this KafkaConsumerConfig config, string? groupId = null)
        {
            var confluentConfig = new ConsumerConfig
            {
                BootstrapServers = config.BootstrapServers,
                GroupId = groupId ?? config.DefaultGroupId,
                AutoOffsetReset = config.AutoOffsetReset,
                EnableAutoCommit = config.EnableAutoCommit,
                AutoCommitIntervalMs = config.AutoCommitIntervalMs,
                SessionTimeoutMs = config.SessionTimeoutMs,
                HeartbeatIntervalMs = config.HeartbeatIntervalMs,
                MaxPollIntervalMs = config.MaxPollIntervalMs,
                FetchMinBytes = config.FetchMinBytes,
                FetchMaxWaitMs = config.FetchMaxWaitMs,
                FetchMaxBytes = config.FetchMaxBytes,
                SecurityProtocol = config.SecurityProtocol
            };

            if (!string.IsNullOrEmpty(config.SaslMechanism))
            {
                confluentConfig.SaslMechanism = Enum.Parse<SaslMechanism>(config.SaslMechanism);
            }

            if (!string.IsNullOrEmpty(config.SaslUsername))
            {
                confluentConfig.SaslUsername = config.SaslUsername;
            }

            if (!string.IsNullOrEmpty(config.SaslPassword))
            {
                confluentConfig.SaslPassword = config.SaslPassword;
            }

            // 追加設定を適用
            foreach (var kvp in config.AdditionalConfig)
            {
                confluentConfig.Set(kvp.Key, kvp.Value?.ToString());
            }

            return confluentConfig;
        }

        /// <summary>
        /// ヘルス状態のサマリ文字列を生成
        /// </summary>
        public static string GetHealthSummary(this KafkaHealthReport report)
        {
            var summary = new List<string>
        {
            $"Overall: {report.HealthLevel}",
            $"Producer: {report.ProducerHealth.HealthLevel}",
            $"Consumer: {report.ConsumerHealth.HealthLevel}",
            $"Avro: {report.AvroHealth.HealthLevel}"
        };

            if (report.Issues.Any())
            {
                summary.Add($"Issues: {report.Issues.Count}");
            }

            return string.Join(" | ", summary);
        }

        /// <summary>
        /// パフォーマンス統計のサマリ文字列を生成
        /// </summary>
        public static string GetPerformanceSummary(this KafkaPerformanceStats stats)
        {
            var summary = new List<string>
        {
            $"Producer: {stats.ProducerStats.TotalMessages} msgs, {stats.ProducerStats.FailureRate:P1} fail",
            $"Consumer: {stats.ConsumerStats.TotalMessages} msgs, {stats.ConsumerStats.FailureRate:P1} fail",
            $"Avro: {stats.AvroStats.BaseStatistics.HitRate:P1} hit rate"
        };

            return string.Join(" | ", summary);
        }
    }
}
