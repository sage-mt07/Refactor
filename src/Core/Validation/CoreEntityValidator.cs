using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Validation
{
    public static class CoreEntityValidator
    {
        public static bool HasCircularReference<T>() where T : class
        {
            var visitedTypes = new HashSet<Type>();
            return HasCircularReferenceInternal(typeof(T), visitedTypes);
        }
        public static bool HasCircularReference(Type entityType)
        {
            var visitedTypes = new HashSet<Type>();
            return HasCircularReferenceInternal(entityType, visitedTypes);
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

                if (underlyingType.IsClass && underlyingType != typeof(string) &&
                    underlyingType.Assembly == type.Assembly)
                {
                    if (HasCircularReferenceInternal(underlyingType, new HashSet<Type>(visitedTypes)))
                    {
                        return true;
                    }
                }
            }

            return false;
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

        public static PropertyInfo[] GetValidProperties<T>() where T : class
        {
            var entityType = typeof(T);
            var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            return Array.FindAll(allProperties, p =>
                p.GetCustomAttribute<KafkaIgnoreAttribute>() == null &&
                IsSerializableType(p.PropertyType));
        }
    }
}
