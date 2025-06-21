using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace KsqlDsl.Serialization.Abstractions;

public class AvroEntityConfiguration
{
    public Type EntityType { get; }
    public string? TopicName { get; set; }
    public PropertyInfo[]? KeyProperties { get; set; }
    public bool ValidateOnStartup { get; set; } = true;
    public bool EnableCaching { get; set; } = true;
    public int? Partitions { get; set; }
    public int? ReplicationFactor { get; set; }
    public Dictionary<string, object> CustomSettings { get; set; } = new();

    public AvroEntityConfiguration(Type entityType)
    {
        EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));

        // 属性からの自動設定
        AutoConfigureFromAttributes();
    }

    /// <summary>
    /// 属性情報からの自動設定
    /// </summary>
    private void AutoConfigureFromAttributes()
    {
        // Topic属性の確認
        var topicAttribute = EntityType.GetCustomAttribute<TopicAttribute>();
        if (topicAttribute != null)
        {
            TopicName = topicAttribute.TopicName;
            if (topicAttribute.PartitionCount > 0)
                Partitions = topicAttribute.PartitionCount;
            if (topicAttribute.ReplicationFactor > 0)
                ReplicationFactor = topicAttribute.ReplicationFactor;
        }

        // Key属性の確認
        var allProperties = EntityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

        if (keyProperties.Length > 0)
        {
            // Order順にソート
            Array.Sort(keyProperties, (p1, p2) =>
            {
                var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                return order1.CompareTo(order2);
            });

            KeyProperties = keyProperties;
        }

        // Stream/Table属性の確認
        var streamAttribute = EntityType.GetCustomAttribute<KsqlStreamAttribute>();
        var tableAttribute = EntityType.GetCustomAttribute<KsqlTableAttribute>();

        if (streamAttribute != null)
        {
            CustomSettings["StreamTableType"] = "Stream";
            if (streamAttribute.Partitions.HasValue)
                Partitions = streamAttribute.Partitions.Value;
        }
        else if (tableAttribute != null)
        {
            CustomSettings["StreamTableType"] = "Table";
            if (tableAttribute.Partitions.HasValue)
                Partitions = tableAttribute.Partitions.Value;
        }
        else
        {
            // デフォルト判定: キーがあればTable、なければStream
            CustomSettings["StreamTableType"] = HasKeys() ? "Table" : "Stream";
        }
    }

    /// <summary>
    /// キープロパティの有無を確認
    /// </summary>
    public bool HasKeys()
    {
        return KeyProperties != null && KeyProperties.Length > 0;
    }

    /// <summary>
    /// 複合キーかどうかを確認
    /// </summary>
    public bool IsCompositeKey()
    {
        return KeyProperties != null && KeyProperties.Length > 1;
    }

    /// <summary>
    /// 順序付きキープロパティを取得
    /// </summary>
    public PropertyInfo[] GetOrderedKeyProperties()
    {
        if (KeyProperties == null || KeyProperties.Length == 0)
            return Array.Empty<PropertyInfo>();

        return KeyProperties
            .OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0)
            .ToArray();
    }

    /// <summary>
    /// シリアライゼーション対象プロパティを取得
    /// KafkaIgnoreAttributeが付いていないプロパティのみ
    /// </summary>
    public PropertyInfo[] GetSerializableProperties()
    {
        var allProperties = EntityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        return Array.FindAll(allProperties, p => p.GetCustomAttribute<KafkaIgnoreAttribute>() == null);
    }

    /// <summary>
    /// 無視されるプロパティを取得
    /// </summary>
    public PropertyInfo[] GetIgnoredProperties()
    {
        var allProperties = EntityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        return Array.FindAll(allProperties, p => p.GetCustomAttribute<KafkaIgnoreAttribute>() != null);
    }

    /// <summary>
    /// キー型を決定
    /// </summary>
    public Type DetermineKeyType()
    {
        if (!HasKeys())
            return typeof(string);

        if (KeyProperties!.Length == 1)
            return KeyProperties[0].PropertyType;

        // 複合キーの場合
        return typeof(Dictionary<string, object>);
    }

    /// <summary>
    /// トピック名を取得（フォールバック付き）
    /// </summary>
    public string GetEffectiveTopicName()
    {
        return TopicName ?? EntityType.Name.ToLowerInvariant();
    }

    /// <summary>
    /// Stream/Table型を取得
    /// </summary>
    public string GetStreamTableType()
    {
        if (CustomSettings.TryGetValue("StreamTableType", out var type) && type is string typeStr)
            return typeStr;

        return HasKeys() ? "Table" : "Stream";
    }

    /// <summary>
    /// 設定の検証
    /// </summary>
    public ValidationResult Validate()
    {
        var result = new ValidationResult { IsValid = true };

        // エンティティ型の基本検証
        if (!EntityType.IsClass || EntityType.IsAbstract)
        {
            result.IsValid = false;
            result.Errors.Add($"Entity type {EntityType.Name} must be a concrete class");
        }

        // トピック名の検証
        var effectiveTopicName = GetEffectiveTopicName();
        if (string.IsNullOrWhiteSpace(effectiveTopicName))
        {
            result.IsValid = false;
            result.Errors.Add("Topic name cannot be null or empty");
        }

        // キープロパティの検証
        if (HasKeys())
        {
            foreach (var keyProperty in KeyProperties!)
            {
                if (!IsValidKeyType(keyProperty.PropertyType))
                {
                    result.Warnings.Add($"Key property {keyProperty.Name} has unsupported type {keyProperty.PropertyType.Name}");
                }
            }
        }

        // パーティション設定の検証
        if (Partitions.HasValue && Partitions.Value <= 0)
        {
            result.IsValid = false;
            result.Errors.Add("Partitions must be greater than 0");
        }

        if (ReplicationFactor.HasValue && ReplicationFactor.Value <= 0)
        {
            result.IsValid = false;
            result.Errors.Add("ReplicationFactor must be greater than 0");
        }

        return result;
    }

    /// <summary>
    /// サポートされているキー型かを確認
    /// </summary>
    private bool IsValidKeyType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        return underlyingType == typeof(string) ||
               underlyingType == typeof(int) ||
               underlyingType == typeof(long) ||
               underlyingType == typeof(Guid) ||
               underlyingType == typeof(byte[]);
    }

    /// <summary>
    /// 設定のコピーを作成
    /// </summary>
    public AvroEntityConfiguration Clone()
    {
        var clone = new AvroEntityConfiguration(EntityType)
        {
            TopicName = TopicName,
            KeyProperties = KeyProperties?.ToArray(),
            ValidateOnStartup = ValidateOnStartup,
            EnableCaching = EnableCaching,
            Partitions = Partitions,
            ReplicationFactor = ReplicationFactor,
            CustomSettings = new Dictionary<string, object>(CustomSettings)
        };

        return clone;
    }

    /// <summary>
    /// 設定の概要を取得
    /// </summary>
    public string GetSummary()
    {
        var topicDisplay = GetEffectiveTopicName();
        var keyCount = KeyProperties?.Length ?? 0;
        var streamTableType = GetStreamTableType();

        return $"Entity: {EntityType.Name} → Topic: {topicDisplay} ({streamTableType}, Keys: {keyCount})";
    }

    public override string ToString()
    {
        return GetSummary();
    }

    public override bool Equals(object? obj)
    {
        if (obj is not AvroEntityConfiguration other)
            return false;

        return EntityType == other.EntityType &&
               TopicName == other.TopicName &&
               ValidateOnStartup == other.ValidateOnStartup;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(EntityType, TopicName, ValidateOnStartup);
    }
}