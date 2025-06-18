using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Models;

// =============================================================================
// Key Classes - キー・識別子クラス
// =============================================================================

/// <summary>
/// Producer識別キー
/// 設計理由：プール管理での効率的なProducer識別
/// </summary>
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