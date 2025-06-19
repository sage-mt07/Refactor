using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace KsqlDsl.Serialization.Avro.Core
{
    public class AvroSerializerFactory
    {
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
        private readonly ILogger<AvroSerializerFactory>? _logger;
        private readonly ILoggerFactory? _loggerFactory;

        public AvroSerializerFactory(
            ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
            ILoggerFactory? loggerFactory = null)
        {
            _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            _loggerFactory = loggerFactory;
            _logger = loggerFactory?.CreateLogger<AvroSerializerFactory>()
                ?? NullLogger<AvroSerializerFactory>.Instance;
        }

        public async Task<SerializerPair<T>> CreateSerializersAsync<T>(EntityModel entityModel, CancellationToken cancellationToken = default) where T : class
        {
            var keySchemaId = await RegisterKeySchemaAsync<T>(entityModel, cancellationToken);
            var valueSchemaId = await RegisterValueSchemaAsync<T>(entityModel, cancellationToken);

            var keySerializer = CreateKeySerializer<T>(entityModel, keySchemaId);
            var valueSerializer = CreateValueSerializer<T>(valueSchemaId);

            return new SerializerPair<T>
            {
                KeySerializer = keySerializer,
                ValueSerializer = valueSerializer,
                KeySchemaId = keySchemaId,
                ValueSchemaId = valueSchemaId
            };
        }

        public async Task<DeserializerPair<T>> CreateDeserializersAsync<T>(EntityModel entityModel, CancellationToken cancellationToken = default) where T : class
        {
            var keySchemaId = await RegisterKeySchemaAsync<T>(entityModel, cancellationToken);
            var valueSchemaId = await RegisterValueSchemaAsync<T>(entityModel, cancellationToken);

            var keyDeserializer = CreateKeyDeserializer<T>(entityModel, keySchemaId);
            var valueDeserializer = CreateValueDeserializer<T>(valueSchemaId);

            return new DeserializerPair<T>
            {
                KeyDeserializer = keyDeserializer,
                ValueDeserializer = valueDeserializer,
                KeySchemaId = keySchemaId,
                ValueSchemaId = valueSchemaId
            };
        }

        public IAvroSerializer<T> CreateSerializer<T>() where T : class
        {
            _logger?.LogDebug("Creating Avro serializer for type {Type}", typeof(T).Name);

            // ✅ 修正: カスタムのAvroSerializerクラスを使用
            return new Core.AvroSerializer<T>(_loggerFactory);
        }

        public IAvroDeserializer<T> CreateDeserializer<T>() where T : class
        {
            _logger?.LogDebug("Creating Avro deserializer for type {Type}", typeof(T).Name);

            // ✅ 修正: カスタムのAvroDeserializerクラスを使用
            return new Core.AvroDeserializer<T>(_loggerFactory);
        }

        private async Task<int> RegisterKeySchemaAsync<T>(EntityModel entityModel, CancellationToken cancellationToken) where T : class
        {
            var topicName = entityModel.TopicAttribute?.TopicName ?? entityModel.EntityType.Name;
            var keyType = DetermineKeyType(entityModel);
            var keySchema = GenerateKeySchema(keyType);

            var subject = $"{topicName}-key";
            var schema = new ConfluentSchemaRegistry.Schema(keySchema, ConfluentSchemaRegistry.SchemaType.Avro);

            // ✅ 修正: CancellationTokenを削除し、normalizeパラメータのみ使用
            return await _schemaRegistryClient.RegisterSchemaAsync(subject, schema, normalize: false);
        }

        private async Task<int> RegisterValueSchemaAsync<T>(EntityModel entityModel, CancellationToken cancellationToken) where T : class
        {
            var topicName = entityModel.TopicAttribute?.TopicName ?? entityModel.EntityType.Name;
            var valueSchema = GenerateValueSchema<T>();

            var subject = $"{topicName}-value";
            var schema = new ConfluentSchemaRegistry.Schema(valueSchema, ConfluentSchemaRegistry.SchemaType.Avro);

            // ✅ 修正: CancellationTokenを削除し、normalizeパラメータのみ使用
            return await _schemaRegistryClient.RegisterSchemaAsync(subject, schema, normalize: false);
        }

        private ISerializer<object> CreateKeySerializer<T>(EntityModel entityModel, int schemaId) where T : class
        {
            var keyType = DetermineKeyType(entityModel);

            if (IsCompositeKey(entityModel))
            {
                return new AvroCompositeKeySerializer(_schemaRegistryClient);
            }
            else
            {
                return CreatePrimitiveKeySerializer(keyType);
            }
        }

        private ISerializer<object> CreateValueSerializer<T>(int schemaId) where T : class
        {
            return new AvroValueSerializer<T>(_schemaRegistryClient);
        }

        private IDeserializer<object> CreateKeyDeserializer<T>(EntityModel entityModel, int schemaId) where T : class
        {
            if (IsCompositeKey(entityModel))
            {
                return new AvroCompositeKeyDeserializer(_schemaRegistryClient);
            }
            else
            {
                var keyType = DetermineKeyType(entityModel);
                return CreatePrimitiveKeyDeserializer(keyType);
            }
        }

        private IDeserializer<object> CreateValueDeserializer<T>(int schemaId) where T : class
        {
            return new AvroValueDeserializer<T>(_schemaRegistryClient);
        }

        private Type DetermineKeyType(EntityModel entityModel)
        {
            if (entityModel.KeyProperties.Length == 0)
                return typeof(string);

            if (entityModel.KeyProperties.Length == 1)
                return entityModel.KeyProperties[0].PropertyType;

            return typeof(System.Collections.Generic.Dictionary<string, object>);
        }

        private bool IsCompositeKey(EntityModel entityModel)
        {
            return entityModel.KeyProperties.Length > 1;
        }

        private string GenerateKeySchema(Type keyType)
        {
            return SchemaGenerator.GenerateKeySchema(keyType);
        }

        private string GenerateValueSchema<T>() where T : class
        {
            return SchemaGenerator.GenerateSchema<T>();
        }

        private ISerializer<object> CreatePrimitiveKeySerializer(Type keyType)
        {
            if (keyType == typeof(string))
                return new StringKeySerializer();
            if (keyType == typeof(int))
                return new IntKeySerializer();
            if (keyType == typeof(long))
                return new LongKeySerializer();
            if (keyType == typeof(Guid))
                return new GuidKeySerializer();

            throw new NotSupportedException($"Key type {keyType.Name} is not supported");
        }

        private IDeserializer<object> CreatePrimitiveKeyDeserializer(Type keyType)
        {
            if (keyType == typeof(string))
                return new StringKeyDeserializer();
            if (keyType == typeof(int))
                return new IntKeyDeserializer();
            if (keyType == typeof(long))
                return new LongKeyDeserializer();
            if (keyType == typeof(Guid))
                return new GuidKeyDeserializer();

            throw new NotSupportedException($"Key type {keyType.Name} is not supported");
        }
    }

    internal class StringKeySerializer : ISerializer<object>
    {
        public byte[] Serialize(object data, SerializationContext context)
        {
            return System.Text.Encoding.UTF8.GetBytes(data?.ToString() ?? "");
        }
    }

    internal class StringKeyDeserializer : IDeserializer<object>
    {
        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return "";
            return System.Text.Encoding.UTF8.GetString(data);
        }
    }

    internal class IntKeySerializer : ISerializer<object>
    {
        public byte[] Serialize(object data, SerializationContext context)
        {
            if (data is int value)
                return BitConverter.GetBytes(value);
            throw new InvalidOperationException($"Cannot serialize {data?.GetType().Name} as int key");
        }
    }

    internal class IntKeyDeserializer : IDeserializer<object>
    {
        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return 0;
            return BitConverter.ToInt32(data);
        }
    }

    internal class LongKeySerializer : ISerializer<object>
    {
        public byte[] Serialize(object data, SerializationContext context)
        {
            if (data is long value)
                return BitConverter.GetBytes(value);
            throw new InvalidOperationException($"Cannot serialize {data?.GetType().Name} as long key");
        }
    }

    internal class LongKeyDeserializer : IDeserializer<object>
    {
        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return 0L;
            return BitConverter.ToInt64(data);
        }
    }

    internal class GuidKeySerializer : ISerializer<object>
    {
        public byte[] Serialize(object data, SerializationContext context)
        {
            if (data is Guid value)
                return value.ToByteArray();
            throw new InvalidOperationException($"Cannot serialize {data?.GetType().Name} as Guid key");
        }
    }

    internal class GuidKeyDeserializer : IDeserializer<object>
    {
        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return Guid.Empty;
            return new Guid(data);
        }
    }

    internal class AvroCompositeKeySerializer : ISerializer<object>
    {
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _client;

        public AvroCompositeKeySerializer(ConfluentSchemaRegistry.ISchemaRegistryClient client)
        {
            _client = client;
        }

        public byte[] Serialize(object data, SerializationContext context)
        {
            if (data is System.Collections.Generic.Dictionary<string, object> dict)
            {
                // ✅ 修正: Confluent.SchemaRegistry.Serdes.AvroSerializerを明示的に指定
                var serializer = new Confluent.SchemaRegistry.Serdes.AvroSerializer<System.Collections.Generic.Dictionary<string, object>>(_client);

                // ✅ 修正: SerializeAsyncを呼び出してGetAwaiter().GetResult()で同期実行
                return serializer.SerializeAsync(dict, context).GetAwaiter().GetResult();
            }
            throw new InvalidOperationException("Expected Dictionary<string, object> for composite key");
        }
    }

    internal class AvroCompositeKeyDeserializer : IDeserializer<object>
    {
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _client;

        public AvroCompositeKeyDeserializer(ConfluentSchemaRegistry.ISchemaRegistryClient client)
        {
            _client = client;
        }

        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return new System.Collections.Generic.Dictionary<string, object>();

            // ✅ 修正: Confluent.SchemaRegistry.Serdes.AvroDeserializerを明示的に指定
            var deserializer = new Confluent.SchemaRegistry.Serdes.AvroDeserializer<System.Collections.Generic.Dictionary<string, object>>(_client);
            var result = deserializer.DeserializeAsync(data.ToArray(), isNull, context).GetAwaiter().GetResult();
            return result!;
        }
    }

    internal class AvroValueSerializer<T> : ISerializer<object> where T : class
    {
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _client;

        public AvroValueSerializer(ConfluentSchemaRegistry.ISchemaRegistryClient client)
        {
            _client = client;
        }

        public byte[] Serialize(object data, SerializationContext context)
        {
            if (data is T typedData)
            {
                // ✅ 修正: Confluent.SchemaRegistry.Serdes.AvroSerializerを明示的に指定
                var serializer = new Confluent.SchemaRegistry.Serdes.AvroSerializer<T>(_client);
                return serializer.SerializeAsync(typedData, context).GetAwaiter().GetResult();
            }
            throw new InvalidOperationException($"Expected type {typeof(T).Name}");
        }
    }

    internal class AvroValueDeserializer<T> : IDeserializer<object> where T : class
    {
        private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _client;

        public AvroValueDeserializer(ConfluentSchemaRegistry.ISchemaRegistryClient client)
        {
            _client = client;
        }

        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            // ✅ 修正: Confluent.SchemaRegistry.Serdes.AvroDeserializerを明示的に指定
            var deserializer = new Confluent.SchemaRegistry.Serdes.AvroDeserializer<T>(_client);
            var result = deserializer.DeserializeAsync(data.ToArray(), isNull, context).GetAwaiter().GetResult();
            return result!;
        }
    }
}