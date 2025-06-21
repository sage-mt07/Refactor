using System;
using System.Reflection;

namespace KsqlDsl.Serialization.Abstractions;

public class AvroEntityConfiguration
{
    public Type EntityType { get; }
    public string? TopicName { get; set; }
    public PropertyInfo[]? KeyProperties { get; set; }
    public bool ValidateOnStartup { get; set; } = true;

    public AvroEntityConfiguration(Type entityType)
    {
        EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
    }

    public bool HasKeys()
    {
        return KeyProperties != null && KeyProperties.Length > 0;
    }

    public bool IsCompositeKey()
    {
        return KeyProperties != null && KeyProperties.Length > 1;
    }

    public override string ToString()
    {
        var topicDisplay = TopicName ?? EntityType.Name;
        var keyCount = KeyProperties?.Length ?? 0;
        return $"Entity: {EntityType.Name} → Topic: {topicDisplay} (Keys: {keyCount})";
    }
}
