using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Serialization.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace KsqlDsl.Core.Models;

public static class KeyExtractor
{
    /// <summary>
    /// エンティティモデルから複合キーかどうかを判定
    /// </summary>
    public static bool IsCompositeKey(EntityModel entityModel)
    {
        return entityModel?.KeyProperties != null && entityModel.KeyProperties.Length > 1;
    }

    /// <summary>
    /// エンティティモデルからキー型を決定
    /// </summary>
    public static Type DetermineKeyType(EntityModel entityModel)
    {
        if (entityModel?.KeyProperties == null || entityModel.KeyProperties.Length == 0)
            return typeof(string);

        if (entityModel.KeyProperties.Length == 1)
            return entityModel.KeyProperties[0].PropertyType;

        // 複合キーの場合
        return typeof(Dictionary<string, object>);
    }

    /// <summary>
    /// エンティティ型からキープロパティを抽出
    /// </summary>
    public static PropertyInfo[] ExtractKeyProperties(Type entityType)
    {
        var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

        // Order順にソート
        Array.Sort(keyProperties, (p1, p2) =>
        {
            var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
            var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
            return order1.CompareTo(order2);
        });

        return keyProperties;
    }

    /// <summary>
    /// エンティティインスタンスからキー値を抽出
    /// </summary>
    public static object ExtractKeyValue<T>(T entity, EntityModel entityModel) where T : class
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        if (entityModel?.KeyProperties == null || entityModel.KeyProperties.Length == 0)
            return entity.GetHashCode().ToString();

        if (entityModel.KeyProperties.Length == 1)
        {
            var keyProperty = entityModel.KeyProperties[0];
            return keyProperty.GetValue(entity) ?? string.Empty;
        }

        // 複合キー
        var keyValues = new Dictionary<string, object>();
        foreach (var property in entityModel.GetOrderedKeyProperties())
        {
            var value = property.GetValue(entity);
            keyValues[property.Name] = value ?? string.Empty;
        }

        return keyValues;
    }

    /// <summary>
    /// キー値を文字列に変換
    /// </summary>
    public static string KeyToString(object keyValue)
    {
        if (keyValue == null)
            return string.Empty;

        if (keyValue is string str)
            return str;

        if (keyValue is Dictionary<string, object> dict)
        {
            var keyPairs = dict.Select(kvp => $"{kvp.Key}={kvp.Value}");
            return string.Join("|", keyPairs);
        }

        return keyValue.ToString() ?? string.Empty;
    }

    /// <summary>
    /// キー型がサポートされているかチェック
    /// </summary>
    public static bool IsSupportedKeyType(Type keyType)
    {
        var underlyingType = Nullable.GetUnderlyingType(keyType) ?? keyType;

        return underlyingType == typeof(string) ||
               underlyingType == typeof(int) ||
               underlyingType == typeof(long) ||
               underlyingType == typeof(Guid) ||
               underlyingType == typeof(byte[]);
    }

    /// <summary>
    /// EntityModelからAvroEntityConfigurationを作成
    /// </summary>
    public static AvroEntityConfiguration ToAvroEntityConfiguration(EntityModel entityModel)
    {
        if (entityModel == null)
            throw new ArgumentNullException(nameof(entityModel));

        var config = new AvroEntityConfiguration(entityModel.EntityType)
        {
            TopicName = entityModel.TopicAttribute?.TopicName,
            KeyProperties = entityModel.KeyProperties
        };

        return config;
    }
}