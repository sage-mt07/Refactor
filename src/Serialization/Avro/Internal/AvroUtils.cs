using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using KsqlDsl.Core.Attributes;
using KsqlDsl.Core.Modeling;
using KsqlDsl.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro.Internal
{
    internal static class AvroUtils
    {
        public static string GetTopicName<T>() where T : class
        {
            return GetTopicName(typeof(T));
        }

        public static string GetTopicName(Type entityType)
        {
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            return topicAttribute?.TopicName ?? entityType.Name;
        }

        public static string GenerateKeySchema<T>() where T : class
        {
            return SchemaGenerator.GenerateKeySchema<T>();
        }

        public static string GenerateValueSchema<T>() where T : class
        {
            return SchemaGenerator.GenerateSchema<T>();
        }

        public static object? ExtractKeyValue<T>(T entity, EntityModel entityModel) where T : class
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));
            if (entityModel == null)
                throw new ArgumentNullException(nameof(entityModel));

            var keyProperties = entityModel.KeyProperties;

            if (keyProperties.Length == 0)
                return null;

            if (keyProperties.Length == 1)
                return keyProperties[0].GetValue(entity);

            var keyRecord = new Dictionary<string, object?>();
            foreach (var prop in keyProperties.OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0))
            {
                keyRecord[prop.Name] = prop.GetValue(entity);
            }
            return keyRecord;
        }

        public static Type DetermineKeyType(EntityModel entityModel)
        {
            if (entityModel == null)
                throw new ArgumentNullException(nameof(entityModel));

            var keyProperties = entityModel.KeyProperties;

            if (keyProperties.Length == 0)
                return typeof(string);

            if (keyProperties.Length == 1)
                return keyProperties[0].PropertyType;

            return typeof(Dictionary<string, object>);
        }

        public static bool IsCompositeKey(EntityModel entityModel)
        {
            return entityModel.KeyProperties.Length > 1;
        }

        public static PropertyInfo[] GetOrderedKeyProperties(EntityModel entityModel)
        {
            return entityModel.KeyProperties
                .OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0)
                .ToArray();
        }

        public static string GetKeySchemaSubject(string topicName)
        {
            return $"{topicName}-key";
        }

        public static string GetValueSchemaSubject(string topicName)
        {
            return $"{topicName}-value";
        }

        public static EntityModel CreateEntityModel<T>() where T : class
        {
            var entityType = typeof(T);
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

        public static bool IsSerializableType(Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            return underlyingType.IsPrimitive ||
                   underlyingType == typeof(string) ||
                   underlyingType == typeof(decimal) ||
                   underlyingType == typeof(DateTime) ||
                   underlyingType == typeof(DateTimeOffset) ||
                   underlyingType == typeof(Guid) ||
                   underlyingType == typeof(byte[]);
        }

        public static bool ValidateEntityForSerialization<T>(T entity) where T : class
        {
            if (entity == null)
                return false;

            var entityType = typeof(T);
            var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in properties)
            {
                if (property.GetCustomAttribute<KafkaIgnoreAttribute>() != null)
                    continue;

                if (!IsSerializableType(property.PropertyType))
                    return false;

                var value = property.GetValue(entity);
                if (value != null && !property.PropertyType.IsAssignableFrom(value.GetType()))
                    return false;
            }

            return true;
        }

        public static Dictionary<string, object> ExtractPropertyValues<T>(T entity) where T : class
        {
            var result = new Dictionary<string, object>();
            var entityType = typeof(T);
            var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in properties)
            {
                if (property.GetCustomAttribute<KafkaIgnoreAttribute>() != null)
                    continue;

                var value = property.GetValue(entity);
                if (value != null)
                {
                    result[property.Name] = value;
                }
            }

            return result;
        }

        public static bool HasCircularReference<T>() where T : class
        {
            var visitedTypes = new HashSet<Type>();
            return HasCircularReferenceInternal(typeof(T), visitedTypes);
        }

        private static bool HasCircularReferenceInternal(Type type, HashSet<Type> visitedTypes)
        {
            if (visitedTypes.Contains(type))
                return true;

            if (type.IsPrimitive || type == typeof(string) || type.IsEnum)
                return false;

            visitedTypes.Add(type);

            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetCustomAttribute<KafkaIgnoreAttribute>() == null);

            foreach (var property in properties)
            {
                var propertyType = property.PropertyType;
                var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

                if (underlyingType.IsClass && underlyingType != typeof(string) && underlyingType.Assembly == type.Assembly)
                {
                    if (HasCircularReferenceInternal(underlyingType, new HashSet<Type>(visitedTypes)))
                    {
                        return true;
                    }
                }
            }

            return false;
        }
    }
}