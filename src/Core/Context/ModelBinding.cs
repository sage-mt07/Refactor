using KsqlDsl.Core.Attributes;
using KsqlDsl.Core.Modeling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace KsqlDsl.Core.Context
{
    /// <summary>
    /// POCOバインド制御
    /// OnModelCreating相当の設定部分を分離
    /// </summary>
    public static class ModelBinding
    {
        public static EntityModel CreateEntityModel<T>() where T : class
        {
            return CreateEntityModel(typeof(T));
        }

        public static EntityModel CreateEntityModel(Type entityType)
        {
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

            Array.Sort(keyProperties, (p1, p2) =>
            {
                var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                return order1.CompareTo(order2);
            });

            return new EntityModel
            {
                EntityType = entityType,
                TopicAttribute = topicAttribute,
                KeyProperties = keyProperties,
                AllProperties = allProperties
            };
        }

        public static Dictionary<string, object> ExtractModelConfiguration(Type entityType)
        {
            var config = new Dictionary<string, object>();

            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            if (topicAttribute != null)
            {
                config["TopicName"] = topicAttribute.TopicName;
                config["PartitionCount"] = topicAttribute.PartitionCount;
                config["ReplicationFactor"] = topicAttribute.ReplicationFactor;
                config["RetentionMs"] = topicAttribute.RetentionMs;
                config["Compaction"] = topicAttribute.Compaction;
                config["DeadLetterQueue"] = topicAttribute.DeadLetterQueue;

                if (topicAttribute.MaxMessageBytes.HasValue)
                    config["MaxMessageBytes"] = topicAttribute.MaxMessageBytes.Value;

                if (topicAttribute.SegmentBytes.HasValue)
                    config["SegmentBytes"] = topicAttribute.SegmentBytes.Value;
            }

            var keyProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetCustomAttribute<KeyAttribute>() != null)
                .ToArray();

            if (keyProperties.Length > 0)
            {
                config["KeyProperties"] = keyProperties.Select(p => new
                {
                    Name = p.Name,
                    Type = p.PropertyType.Name,
                    Order = p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0
                }).ToArray();
            }

            return config;
        }

        public static string GetConfigurationSummary(Type entityType)
        {
            var config = ExtractModelConfiguration(entityType);
            var summary = new List<string> { $"Entity: {entityType.Name}" };

            foreach (var kvp in config)
            {
                summary.Add($"  {kvp.Key}: {kvp.Value}");
            }

            return string.Join(Environment.NewLine, summary);
        }
    }
}