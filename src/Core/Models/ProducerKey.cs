using System;

namespace KsqlDsl.Core.Models;


public class ProducerKey : IEquatable<ProducerKey>
{
    public Type EntityType { get; }
    public string TopicName { get; }
    public int ConfigurationHash { get; }

    public ProducerKey(Type entityType, string topicName, int configurationHash)
    {
        EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
        TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
        ConfigurationHash = configurationHash;
    }

    public override int GetHashCode() => HashCode.Combine(EntityType, TopicName, ConfigurationHash);

    public bool Equals(ProducerKey? other)
    {
        return other != null &&
               EntityType == other.EntityType &&
               TopicName == other.TopicName &&
               ConfigurationHash == other.ConfigurationHash;
    }

    public override bool Equals(object? obj) => obj is ProducerKey other && Equals(other);

    public override string ToString() => $"{EntityType.Name}:{TopicName}:{ConfigurationHash:X8}";
}