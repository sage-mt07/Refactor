using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Management;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Application
{
    public abstract class KsqlContext : IDisposable
    {
        private readonly IAvroSchemaRegistrationService _schemaService;
        private readonly IAvroSerializationManager _serializationManager;
        private readonly IAvroSchemaRepository _schemaRepository;
        private readonly ILoggerFactory? _loggerFactory;
        private readonly ILogger<KsqlContext> _logger;
        private readonly KsqlContextOptions _options;
        private bool _disposed = false;

        protected KsqlContext(KsqlContextOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _loggerFactory = options.LoggerFactory;
            _logger = _loggerFactory.CreateLoggerOrNull<KsqlContext>();

            _schemaRepository = new AvroSchemaRepository();
            _schemaService = new AvroSchemaRegistrationService(
                options.SchemaRegistryClient, 
                _loggerFactory);
            _serializationManager = new AvroSerializationManager(
                options.SchemaRegistryClient, 
                _schemaRepository);
        }

        protected abstract void OnAvroModelCreating(AvroModelBuilder modelBuilder);

        public async Task InitializeAsync()
        {
            var modelBuilder = new AvroModelBuilder();
            OnAvroModelCreating(modelBuilder);

            var configurations = modelBuilder.Build();

            try
            {
                await _schemaService.RegisterAllSchemasAsync(configurations);

                foreach (var (entityType, config) in configurations)
                {
                    var schemaInfo = await _schemaService.GetSchemaInfoAsync(entityType);
                    _schemaRepository.StoreSchemaInfo(schemaInfo);
                }

                await _serializationManager.PreWarmCacheAsync();

                _logger.LogInformationWithLegacySupport(_loggerFactory, false,
                    "AVRO initialization completed: {EntityCount} entities", configurations.Count);
            }
            catch (Exception ex)
            {
                _logger.LogErrorWithLegacySupport(ex, _loggerFactory, false,
                    "AVRO initialization failed");
                throw;
            }
        }

        public IAvroSerializer<T> GetSerializer<T>() where T : class
        {
            return _serializationManager.GetSerializer<T>();
        }

        public IAvroDeserializer<T> GetDeserializer<T>() where T : class
        {
            return _serializationManager.GetDeserializer<T>();
        }

        public AvroSchemaInfo? GetSchemaInfo<T>() where T : class
        {
            return _schemaRepository.GetSchemaInfo(typeof(T));
        }

        public AvroSchemaInfo? GetSchemaInfoByTopic(string topicName)
        {
            return _schemaRepository.GetSchemaInfoByTopic(topicName);
        }

        public bool IsEntityRegistered<T>() where T : class
        {
            return _schemaRepository.IsRegistered(typeof(T));
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _serializationManager?.Dispose();
                _schemaRepository?.Clear();
                _disposed = true;
            }
        }
    }

    public class KsqlContextOptions
    {
        public ConfluentSchemaRegistry.ISchemaRegistryClient SchemaRegistryClient { get; set; } = null!;
        public ILoggerFactory? LoggerFactory { get; set; }
        public bool EnableDebugLogging { get; set; } = false;
        public bool ValidateOnStartup { get; set; } = true;

        public void Validate()
        {
            if (SchemaRegistryClient == null)
                throw new InvalidOperationException("SchemaRegistryClient is required");
        }
    }

    public interface IAvroSerializationManager
    {
        Task PreWarmCacheAsync();
        IAvroSerializer<T> GetSerializer<T>() where T : class;
        IAvroDeserializer<T> GetDeserializer<T>() where T : class;
        void Dispose();
    }

    public class AvroSerializationManager : IAvroSerializationManager
    {
        private readonly IAvroSchemaRepository _schemaRepository;
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
        private readonly Dictionary<Type, object> _serializers = new();
        private readonly Dictionary<Type, object> _deserializers = new();

        public AvroSerializationManager(
            ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
            IAvroSchemaRepository schemaRepository)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _schemaRepository = schemaRepository ?? throw new ArgumentNullException(nameof(schemaRepository));
        }

        public async Task PreWarmCacheAsync()
        {
            var allSchemas = _schemaRepository.GetAllSchemas();
            
            foreach (var schemaInfo in allSchemas)
            {
                await PreWarmEntityAsync(schemaInfo);
            }
        }

        private async Task PreWarmEntityAsync(AvroSchemaInfo schemaInfo)
        {
            var entityType = schemaInfo.EntityType;
            
            var serializerType = typeof(IAvroSerializer<>).MakeGenericType(entityType);
            var deserializerType = typeof(IAvroDeserializer<>).MakeGenericType(entityType);
            
            var serializer = CreateSerializer(entityType);
            var deserializer = CreateDeserializer(entityType);
            
            _serializers[entityType] = serializer;
            _deserializers[entityType] = deserializer;
            
            await Task.CompletedTask;
        }

        public IAvroSerializer<T> GetSerializer<T>() where T : class
        {
            var entityType = typeof(T);
            
            if (_serializers.TryGetValue(entityType, out var serializer))
            {
                return (IAvroSerializer<T>)serializer;
            }
            
            var newSerializer = CreateSerializer<T>();
            _serializers[entityType] = newSerializer;
            return newSerializer;
        }

        public IAvroDeserializer<T> GetDeserializer<T>() where T : class
        {
            var entityType = typeof(T);
            
            if (_deserializers.TryGetValue(entityType, out var deserializer))
            {
                return (IAvroDeserializer<T>)deserializer;
            }
            
            var newDeserializer = CreateDeserializer<T>();
            _deserializers[entityType] = newDeserializer;
            return newDeserializer;
        }

        private object CreateSerializer(Type entityType)
        {
            var serializerType = typeof(Confluent.SchemaRegistry.Serdes.AvroSerializer<>).MakeGenericType(entityType);
            return Activator.CreateInstance(serializerType, _schemaRegistryClient)!;
        }

        private IAvroSerializer<T> CreateSerializer<T>() where T : class
        {
            return new Confluent.SchemaRegistry.Serdes.AvroSerializer<T>(_schemaRegistryClient);
        }

        private object CreateDeserializer(Type entityType)
        {
            var deserializerType = typeof(Confluent.SchemaRegistry.Serdes.AvroDeserializer<>).MakeGenericType(entityType);
            return Activator.CreateInstance(deserializerType, _schemaRegistryClient)!;
        }

        private IAvroDeserializer<T> CreateDeserializer<T>() where T : class
        {
            return new Confluent.SchemaRegistry.Serdes.AvroDeserializer<T>(_schemaRegistryClient);
        }

        public void Dispose()
        {
            foreach (var serializer in _serializers.Values)
            {
                if (serializer is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _serializers.Clear();

            foreach (var deserializer in _deserializers.Values)
            {
                if (deserializer is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _deserializers.Clear();
        }
    }
}