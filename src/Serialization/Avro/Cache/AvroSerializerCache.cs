using Confluent.Kafka;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Cache
{
    public class AvroSerializerCache : IDisposable
    {
        private readonly ConcurrentDictionary<Type, object> _serializerManagers = new();
        private readonly AvroSerializerFactory _factory;
        private readonly ILogger<AvroSerializerCache>? _logger;
        private bool _disposed = false;

        public AvroSerializerCache(
            AvroSerializerFactory factory,
            ILoggerFactory? logger = null)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _logger = logger?.CreateLogger<AvroSerializerCache>()
                ?? NullLogger<AvroSerializerCache>.Instance;
        }

        public IAvroSerializationManager<T> GetAvroManager<T>() where T : class
        {
            var entityType = typeof(T);

            if (_serializerManagers.TryGetValue(entityType, out var existingManager))
            {
                return (IAvroSerializationManager<T>)existingManager;
            }

            var newManager = new AvroEntitySerializationManager<T>(_factory, _logger);
            _serializerManagers[entityType] = newManager;
            return newManager;
        }

        public IAvroSerializer<T> GetOrCreateSerializer<T>() where T : class
        {
            _logger?.LogDebug("Creating Avro serializer for type {Type}", typeof(T).Name);
            return _factory.CreateSerializer<T>();
        }

        public IAvroDeserializer<T> GetOrCreateDeserializer<T>() where T : class
        {
            _logger?.LogDebug("Creating Avro deserializer for type {Type}", typeof(T).Name);
            return _factory.CreateDeserializer<T>();
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
                foreach (var manager in _serializerManagers.Values)
                {
                    if (manager is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }
                _serializerManagers.Clear();
                _disposed = true;
            }
        }

        internal class AvroEntitySerializationManager<T> : IAvroSerializationManager<T> where T : class
        {
            private readonly AvroSerializerFactory _factory;
            private readonly ILogger? _logger;
            private readonly ConcurrentDictionary<string, SerializerPair<T>> _serializerCache = new();
            private readonly ConcurrentDictionary<string, DeserializerPair<T>> _deserializerCache = new();
            private readonly SerializationStatistics _statistics = new();
            private bool _disposed = false;

            public Type EntityType => typeof(T);

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
                return _statistics;
            }

            private string GenerateCacheKey()
            {
                return $"{typeof(T).FullName}:avro";
            }

            private EntityModel GetEntityModel<TEntity>() where TEntity : class
            {
                var entityType = typeof(TEntity);
                var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
                var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
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
}