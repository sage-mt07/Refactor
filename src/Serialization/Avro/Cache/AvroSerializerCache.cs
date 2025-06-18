using Confluent.Kafka;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Cache
{
    public class AvroSerializerCache : ISerializationManager<object>, IDisposable
    {
        private readonly ConcurrentDictionary<Type, object> _serializerManagers = new();
        private readonly AvroSerializerFactory _factory;
        private readonly ILogger<AvroSerializerCache>? _logger;
        private readonly SerializationStatistics _statistics = new();
        private bool _disposed = false;
        private readonly Dictionary<string, object> _deserializerCache = new();
        private readonly Dictionary<string, object> _serializerCache = new();

        public Type EntityType => typeof(object);
      

        public AvroSerializerCache(
            AvroSerializerFactory factory,
            ILoggerFactory? logger = null)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _logger = logger?.CreateLogger<AvroSerializerCache>()
                ?? NullLogger<AvroSerializerCache>.Instance;
        }

        public ISerializationManager<T> GetManager<T>() where T : class
        {
            var entityType = typeof(T);

            if (_serializerManagers.TryGetValue(entityType, out var existingManager))
            {
                return (ISerializationManager<T>)existingManager;
            }

            var newManager = new AvroEntitySerializationManager<T>(_factory, _logger);
            _serializerManagers[entityType] = newManager;
            return newManager;
        }
        // AvroSerializerCache.cs に追加
        public virtual IAvroSerializer<T> GetOrCreateSerializer<T>() where T : class
        {
            var key = GetSerializerCacheKey<T>();
            if (_serializerCache.TryGetValue(key, out var cached))
            {
                return (IAvroSerializer<T>)cached;
            }

            var serializer = _factory.CreateSerializer<T>();
            _serializerCache[key] = serializer;

            _logger?.LogDebug("Created serializer for type {Type}", typeof(T).Name);

            return serializer;
        }

        private string GetSerializerCacheKey<T>()
        {
            return $"serializer:{typeof(T).FullName}";
        }

        // AvroSerializerCache.cs に追加
        public virtual IAvroDeserializer<T> GetOrCreateDeserializer<T>() where T : class
        {
            var key = GetDeserializerCacheKey<T>();

            if (_deserializerCache.TryGetValue(key, out var cached))
            {
                return (IAvroDeserializer<T>)cached;
            }

            var deserializer = _factory.CreateDeserializer<T>();
            _deserializerCache[key] = deserializer;

            _logger?.LogDebug("Created deserializer for type {Type}", typeof(T).Name);

            return deserializer;
        }

        private string GetDeserializerCacheKey<T>()
        {
            return $"deserializer:{typeof(T).FullName}";
        }
        public Task<SerializerPair<object>> GetSerializersAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Use GetManager<T>() for type-specific serializers");
        }

        public Task<DeserializerPair<object>> GetDeserializersAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Use GetManager<T>() for type-specific deserializers");
        }

        public Task<bool> ValidateRoundTripAsync(object entity, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Use GetManager<T>() for type-specific validation");
        }

        public SerializationStatistics GetStatistics()
        {
            var aggregated = new SerializationStatistics();

            foreach (var manager in _serializerManagers.Values)
            {
                if (manager is ISerializationManager<object> typedManager)
                {
                    var stats = typedManager.GetStatistics();
                    aggregated.TotalSerializations += stats.TotalSerializations;
                    aggregated.TotalDeserializations += stats.TotalDeserializations;
                    aggregated.CacheHits += stats.CacheHits;
                    aggregated.CacheMisses += stats.CacheMisses;
                }
            }

            aggregated.LastUpdated = DateTime.UtcNow;
            return aggregated;
        }

        public void ClearCache()
        {
            foreach (var manager in _serializerManagers.Values)
            {
                if (manager is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _serializerManagers.Clear();
        }

        public void ClearCache<T>() where T : class
        {
            var entityType = typeof(T);
            if (_serializerManagers.TryRemove(entityType, out var manager))
            {
                if (manager is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                ClearCache();
                _disposed = true;
            }
        }
    }

    internal class AvroEntitySerializationManager<T> : ISerializationManager<T> where T : class
    {
        private readonly AvroSerializerFactory _factory;
        private readonly ILogger? _logger;
        private readonly ConcurrentDictionary<string, SerializerPair<T>> _serializerCache = new();
        private readonly ConcurrentDictionary<string, DeserializerPair<T>> _deserializerCache = new();
        private readonly SerializationStatistics _statistics = new();
        private bool _disposed = false;

        public Type EntityType => typeof(T);
        public SerializationFormat Format => SerializationFormat.Avro;

        public AvroEntitySerializationManager(
            AvroSerializerFactory factory,
            ILogger? logger = null)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _logger = logger;
        }

        public async Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default)
        {
            var cacheKey = GenerateCacheKey();

            if (_serializerCache.TryGetValue(cacheKey, out var cached))
            {
                Interlocked.Increment(ref _statistics.CacheHits);
                return cached;
            }

            Interlocked.Increment(ref _statistics.CacheMisses);

            var entityModel = GetEntityModel<T>();
            var serializers = await _factory.CreateSerializersAsync<T>(entityModel, cancellationToken);

            _serializerCache[cacheKey] = serializers;
            Interlocked.Increment(ref _statistics.TotalSerializations);

            return serializers;
        }

        public async Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default)
        {
            var cacheKey = GenerateCacheKey();

            if (_deserializerCache.TryGetValue(cacheKey, out var cached))
            {
                Interlocked.Increment(ref _statistics.CacheHits);
                return cached;
            }

            Interlocked.Increment(ref _statistics.CacheMisses);

            var entityModel = GetEntityModel<T>();
            var deserializers = await _factory.CreateDeserializersAsync<T>(entityModel, cancellationToken);

            _deserializerCache[cacheKey] = deserializers;
            Interlocked.Increment(ref _statistics.TotalDeserializations);

            return deserializers;
        }

        public async Task<bool> ValidateRoundTripAsync(T entity, CancellationToken cancellationToken = default)
        {
            try
            {
                var serializers = await GetSerializersAsync(cancellationToken);
                var deserializers = await GetDeserializersAsync(cancellationToken);

                var context = new SerializationContext(MessageComponentType.Value, typeof(T).Name);

                var serializedValue = serializers.ValueSerializer.Serialize(entity, context);
                var deserializedValue = deserializers.ValueDeserializer.Deserialize(serializedValue, false, context);

                return deserializedValue != null && deserializedValue.GetType() == typeof(T);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Round-trip validation failed for {EntityType}", typeof(T).Name);
                return false;
            }
        }

        public SerializationStatistics GetStatistics()
        {
            lock (_statistics)
            {
                return new SerializationStatistics
                {
                    TotalSerializations = _statistics.TotalSerializations,
                    TotalDeserializations = _statistics.TotalDeserializations,
                    CacheHits = _statistics.CacheHits,
                    CacheMisses = _statistics.CacheMisses,
                    AverageLatency = _statistics.AverageLatency,
                    LastUpdated = DateTime.UtcNow
                };
            }
        }

        private string GenerateCacheKey()
        {
            return $"{typeof(T).FullName}:avro";
        }

        private EntityModel GetEntityModel<TEntity>() where TEntity : class
        {
            var entityType = typeof(TEntity);
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            var allProperties = entityType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

            return new EntityModel
            {
                EntityType = entityType,
                TopicAttribute = topicAttribute,
                KeyProperties = keyProperties,
                AllProperties = allProperties
            };
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _serializerCache.Clear();
                _deserializerCache.Clear();
                _disposed = true;
            }
        }
    }
}