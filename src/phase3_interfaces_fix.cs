using KsqlDsl.Serialization.Avro.Abstractions;
using System;
using System.Threading.Tasks;

namespace KsqlDsl.Application
{
    // AvroSchemaRegistrationServiceの拡張インターフェース
    public interface IAvroSchemaRegistrationService
    {
        Task RegisterAllSchemasAsync(IReadOnlyDictionary<Type, AvroEntityConfiguration> configurations);
        Task<AvroSchemaInfo> GetSchemaInfoAsync<T>() where T : class;
        Task<AvroSchemaInfo> GetSchemaInfoAsync(Type entityType);
        Task<List<AvroSchemaInfo>> GetAllRegisteredSchemasAsync();
    }

    // 型安全なAvroSerializationManagerの拡張
    public static class AvroSchemaRegistrationServiceExtensions
    {
        public static async Task<AvroSchemaInfo> GetSchemaInfoAsync(
            this IAvroSchemaRegistrationService service, 
            Type entityType)
        {
            var method = typeof(IAvroSchemaRegistrationService)
                .GetMethod(nameof(IAvroSchemaRegistrationService.GetSchemaInfoAsync))!
                .MakeGenericMethod(entityType);
            
            var task = (Task<AvroSchemaInfo>)method.Invoke(service, null)!;
            return await task;
        }
    }

    // スキーマ情報拡張クラス
    public static class AvroSchemaInfoExtensions
    {
        public static string GetKeySubject(this AvroSchemaInfo schemaInfo)
        {
            return $"{schemaInfo.TopicName}-key";
        }

        public static string GetValueSubject(this AvroSchemaInfo schemaInfo)
        {
            return $"{schemaInfo.TopicName}-value";
        }

        public static bool IsCompositeKey(this AvroSchemaInfo schemaInfo)
        {
            return schemaInfo.HasCustomKey && 
                   schemaInfo.KeyProperties != null && 
                   schemaInfo.KeyProperties.Length > 1;
        }

        public static string GetKeyTypeName(this AvroSchemaInfo schemaInfo)
        {
            if (!schemaInfo.HasCustomKey)
                return "string";

            if (schemaInfo.KeyProperties?.Length == 1)
                return schemaInfo.KeyProperties[0].PropertyType.Name;

            return "CompositeKey";
        }
    }

    // コンテキストオプション拡張
    public static class KsqlContextOptionsExtensions
    {
        public static KsqlContextOptions UseSchemaRegistry(
            this KsqlContextOptions options,
            string url)
        {
            var config = new Confluent.SchemaRegistry.SchemaRegistryConfig { Url = url };
            options.SchemaRegistryClient = new Confluent.SchemaRegistry.CachedSchemaRegistryClient(config);
            return options;
        }

        public static KsqlContextOptions EnableLogging(
            this KsqlContextOptions options,
            ILoggerFactory loggerFactory)
        {
            options.LoggerFactory = loggerFactory;
            return options;
        }

        public static KsqlContextOptions ConfigureValidation(
            this KsqlContextOptions options,
            bool validateOnStartup = true,
            bool enableDebugLogging = false)
        {
            options.ValidateOnStartup = validateOnStartup;
            options.EnableDebugLogging = enableDebugLogging;
            return options;
        }
    }

    // エンティティビルダー拡張
    public static class AvroEntityTypeBuilderExtensions
    {
        public static AvroEntityTypeBuilder<T> UseDefaultTopic<T>(
            this AvroEntityTypeBuilder<T> builder) where T : class
        {
            var entityType = typeof(T);
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            
            if (topicAttribute != null)
            {
                builder.ToTopic(topicAttribute.TopicName);
            }
            else
            {
                builder.ToTopic(entityType.Name.ToLowerInvariant());
            }
            
            return builder;
        }

        public static AvroEntityTypeBuilder<T> UseKeyFromAttribute<T>(
            this AvroEntityTypeBuilder<T> builder) where T : class
        {
            var entityType = typeof(T);
            var keyProperties = entityType.GetProperties()
                .Where(p => p.GetCustomAttribute<KeyAttribute>() != null)
                .OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0)
                .ToArray();

            if (keyProperties.Length > 0)
            {
                // 動的にキー式を構築する（簡略化版）
                // 実際の実装では Expression.Lambda を使用
                builder.HasKey(CreateKeyExpression<T>(keyProperties));
            }

            return builder;
        }

        private static Expression<Func<T, object>> CreateKeyExpression<T>(PropertyInfo[] keyProperties) where T : class
        {
            var parameter = Expression.Parameter(typeof(T), "x");
            
            if (keyProperties.Length == 1)
            {
                var property = Expression.Property(parameter, keyProperties[0]);
                var converted = Expression.Convert(property, typeof(object));
                return Expression.Lambda<Func<T, object>>(converted, parameter);
            }
            else
            {
                // 複合キーの場合は匿名型を作成
                var properties = keyProperties.Select(p => Expression.Property(parameter, p)).ToArray();
                var newExpression = Expression.New(
                    typeof(object).GetConstructor(Type.EmptyTypes)!);
                return Expression.Lambda<Func<T, object>>(newExpression, parameter);
            }
        }
    }
}