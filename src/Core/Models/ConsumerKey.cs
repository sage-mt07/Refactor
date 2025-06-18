using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Models
{

    /// <summary>
    /// Consumer識別キー
    /// 設計理由：プール管理での効率的なConsumer識別
    /// </summary>
    public class ConsumerKey : IEquatable<ConsumerKey>
    {
        public Type EntityType { get; }
        public string TopicName { get; }
        public string GroupId { get; }

        public ConsumerKey(Type entityType, string topicName, string groupId)
        {
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            GroupId = groupId ?? throw new ArgumentNullException(nameof(groupId));
        }

        public override int GetHashCode() => HashCode.Combine(EntityType, TopicName, GroupId);

        public bool Equals(ConsumerKey? other)
        {
            return other != null &&
                   EntityType == other.EntityType &&
                   TopicName == other.TopicName &&
                   GroupId == other.GroupId;
        }

        public override bool Equals(object? obj) => obj is ConsumerKey other && Equals(other);

        public override string ToString() => $"{EntityType.Name}:{TopicName}:{GroupId}";
    }

}
