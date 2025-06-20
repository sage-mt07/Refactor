using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Configuration;
using KsqlDsl.Core.Exceptions;
using KsqlDsl.Core.Validation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace KsqlDsl.Core.Factories
{
    public class CoreEntityFactory : ICoreEntityFactory
    {
        private readonly ICoreSettingsProvider _settingsProvider;
        private readonly Dictionary<Type, EntityModel> _cache = new();
        private readonly object _lock = new();

        public CoreEntityFactory(ICoreSettingsProvider settingsProvider)
        {
            _settingsProvider = settingsProvider ?? throw new ArgumentNullException(nameof(settingsProvider));
        }

        public EntityModel CreateEntityModel<T>() where T : class
        {
            return CreateEntityModel(typeof(T));
        }

        public EntityModel CreateEntityModel(Type entityType)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            if (!CanCreateEntityModel(entityType))
                throw new EntityModelException(entityType, "Type is not supported for entity model creation");

            var settings = _settingsProvider.GetSettings();

            lock (_lock)
            {
                if (settings.EnableEntityCaching && _cache.TryGetValue(entityType, out var cachedModel))
                {
                    return cachedModel;
                }

                var entityModel = CreateEntityModelInternal(entityType, settings);

                if (settings.EnableEntityCaching)
                {
                    // Simple cache size management
                    if (_cache.Count >= settings.MaxEntityCacheSize)
                    {
                        _cache.Clear(); // Simple eviction strategy
                    }
                    _cache[entityType] = entityModel;
                }

                return entityModel;
            }
        }

        public bool CanCreateEntityModel(Type entityType)
        {
            if (entityType == null) return false;
            if (!entityType.IsClass) return false;
            if (entityType.IsAbstract) return false;
            if (entityType.IsGenericType) return false;

            // Must have at least one serializable property
            var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            return properties.Any(p => p.GetCustomAttribute<KafkaIgnoreAttribute>() == null);
        }

        public List<Type> GetSupportedEntityTypes()
        {
            return AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(a => a.GetTypes())
                .Where(CanCreateEntityModel)
                .ToList();
        }

        private EntityModel CreateEntityModelInternal(Type entityType, CoreSettings settings)
        {
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

            // Sort key properties by order
            Array.Sort(keyProperties, (p1, p2) =>
            {
                var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                return order1.CompareTo(order2);
            });

            var entityModel = new EntityModel
            {
                EntityType = entityType,
                TopicAttribute = topicAttribute,
                KeyProperties = keyProperties,
                AllProperties = allProperties
            };

            // Core validation
            if (settings.EnableCircularReferenceCheck &&
                CoreEntityValidator.HasCircularReference(entityType))
            {
                throw new EntityModelException(entityType, "Circular reference detected");
            }

            if (settings.EnablePropertyValidation)
            {
                var invalidProperties = allProperties
                    .Where(p => p.GetCustomAttribute<KafkaIgnoreAttribute>() == null)
                    .Where(p => !CoreEntityValidator.IsSerializableType(p.PropertyType))
                    .ToList();

                if (invalidProperties.Any())
                {
                    var propertyNames = string.Join(", ", invalidProperties.Select(p => p.Name));
                    throw new EntityModelException(entityType,
                        $"Invalid property types detected: {propertyNames}");
                }
            }

            return entityModel;
        }
    }
}
