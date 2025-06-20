using KsqlDsl.Core.Abstractions;
using System;
using System.Linq;
using System.Reflection;

namespace KsqlDsl.Core.Extensions
{
    public static class CoreExtensions
    {
        /// <summary>
        /// EntityModel拡張メソッド
        /// </summary>
        public static string GetTopicName(this EntityModel entityModel)
        {
            return entityModel.TopicAttribute?.TopicName ?? entityModel.EntityType.Name;
        }

        public static bool HasKeys(this EntityModel entityModel)
        {
            return entityModel.KeyProperties.Length > 0;
        }

        public static bool IsCompositeKey(this EntityModel entityModel)
        {
            return entityModel.KeyProperties.Length > 1;
        }

        public static PropertyInfo[] GetOrderedKeyProperties(this EntityModel entityModel)
        {
            return entityModel.KeyProperties
                .OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0)
                .ToArray();
        }

        public static PropertyInfo[] GetSerializableProperties(this EntityModel entityModel)
        {
            return Array.FindAll(entityModel.AllProperties, p =>
                p.GetCustomAttribute<KafkaIgnoreAttribute>() == null);
        }

        /// <summary>
        /// Type拡張メソッド
        /// </summary>
        public static bool IsKafkaEntity(this Type type)
        {
            return type.IsClass &&
                   !type.IsAbstract &&
                   type.GetCustomAttribute<TopicAttribute>() != null;
        }

        public static string GetKafkaTopicName(this Type type)
        {
            var topicAttribute = type.GetCustomAttribute<TopicAttribute>();
            return topicAttribute?.TopicName ?? type.Name;
        }

        public static bool HasKafkaKeys(this Type type)
        {
            return type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Any(p => p.GetCustomAttribute<KeyAttribute>() != null);
        }

        /// <summary>
        /// PropertyInfo拡張メソッド
        /// </summary>
        public static bool IsKafkaIgnored(this PropertyInfo property)
        {
            return property.GetCustomAttribute<KafkaIgnoreAttribute>() != null;
        }

        public static bool IsKafkaKey(this PropertyInfo property)
        {
            return property.GetCustomAttribute<KeyAttribute>() != null;
        }

        public static int GetKeyOrder(this PropertyInfo property)
        {
            return property.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
        }

        public static bool IsNullableProperty(this PropertyInfo property)
        {
            var propertyType = property.PropertyType;

            // Nullable value types
            if (Nullable.GetUnderlyingType(propertyType) != null)
                return true;

            // Value types are non-nullable by default
            if (propertyType.IsValueType)
                return false;

            // Reference types - check nullable context
            try
            {
                var nullabilityContext = new NullabilityInfoContext();
                var nullabilityInfo = nullabilityContext.Create(property);
                return nullabilityInfo.WriteState == NullabilityState.Nullable ||
                       nullabilityInfo.ReadState == NullabilityState.Nullable;
            }
            catch
            {
                // Fallback: assume reference types are nullable
                return !propertyType.IsValueType;
            }
        }
    }
}
