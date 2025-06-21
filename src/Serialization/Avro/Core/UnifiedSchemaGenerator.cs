using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;

namespace KsqlDsl.Serialization.Avro.Core
{
    public static class UnifiedSchemaGenerator
    {
        #region Core Schema Generation

        /// <summary>
        /// 型からAvroスキーマを生成
        /// </summary>
        public static string GenerateSchema<T>()
        {
            return GenerateSchema(typeof(T));
        }

        /// <summary>
        /// 型からAvroスキーマを生成
        /// </summary>
        public static string GenerateSchema(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var schema = new AvroSchema
            {
                Type = "record",
                Name = type.Name,
                Namespace = type.Namespace ?? "KsqlDsl.Generated",
                Fields = GenerateFields(type)
            };

            return SerializeSchema(schema);
        }

        /// <summary>
        /// オプション付きスキーマ生成
        /// </summary>
        public static string GenerateSchema(Type type, SchemaGenerationOptions options)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            var schema = new AvroSchema
            {
                Type = "record",
                Name = options.CustomName ?? type.Name,
                Namespace = options.Namespace ?? type.Namespace ?? "KsqlDsl.Generated",
                Doc = options.Documentation,
                Fields = GenerateFields(type)
            };

            return SerializeSchema(schema, options);
        }

        /// <summary>
        /// AvroEntityConfigurationからスキーマ生成
        /// </summary>
        public static string GenerateSchema(AvroEntityConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var entityType = configuration.EntityType;
            var topicName = configuration.GetEffectiveTopicName();

            var schema = new AvroSchema
            {
                Type = "record",
                Name = $"{ToPascalCase(topicName)}_value",
                Namespace = $"{entityType.Namespace}.Avro",
                Fields = GenerateFieldsFromConfiguration(configuration)
            };

            return SerializeSchema(schema);
        }

        #endregion

        #region Key Schema Generation

        /// <summary>
        /// キースキーマ生成
        /// </summary>
        public static string GenerateKeySchema<T>()
        {
            return GenerateKeySchema(typeof(T));
        }

        /// <summary>
        /// キースキーマ生成
        /// </summary>
        public static string GenerateKeySchema(Type keyType)
        {
            if (keyType == null)
                throw new ArgumentNullException(nameof(keyType));

            // プリミティブ型の直接処理
            if (IsPrimitiveType(keyType))
            {
                return GeneratePrimitiveKeySchema(keyType);
            }

            // Nullable プリミティブ型
            var underlyingType = Nullable.GetUnderlyingType(keyType);
            if (underlyingType != null && IsPrimitiveType(underlyingType))
            {
                return GenerateNullablePrimitiveKeySchema(underlyingType);
            }

            // 複合型をレコードとして処理
            var schema = new AvroSchema
            {
                Type = "record",
                Name = $"{keyType.Name}Key",
                Namespace = keyType.Namespace ?? "KsqlDsl.Generated",
                Fields = GenerateFields(keyType)
            };

            return SerializeSchema(schema);
        }

        /// <summary>
        /// AvroEntityConfigurationからキースキーマ生成
        /// </summary>
        public static string GenerateKeySchema(AvroEntityConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            if (!configuration.HasKeys())
                return GeneratePrimitiveKeySchema(typeof(string));

            if (configuration.KeyProperties!.Length == 1)
                return GenerateKeySchema(configuration.KeyProperties[0].PropertyType);

            // 複合キー
            return GenerateCompositeKeySchema(configuration.GetOrderedKeyProperties());
        }

        /// <summary>
        /// エンティティ型とConfigurationからキースキーマ生成
        /// </summary>
        public static string GenerateKeySchema(Type entityType, AvroEntityConfiguration config)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            if (config.KeyProperties == null || config.KeyProperties.Length == 0)
                return GeneratePrimitiveKeySchema(typeof(string));

            if (config.KeyProperties.Length == 1)
                return GenerateKeySchema(config.KeyProperties[0].PropertyType);

            return GenerateCompositeKeySchema(config.KeyProperties);
        }

        #endregion

        #region Value Schema Generation

        /// <summary>
        /// バリュースキーマ生成
        /// </summary>
        public static string GenerateValueSchema<T>()
        {
            return GenerateValueSchema(typeof(T));
        }

        /// <summary>
        /// バリュースキーマ生成
        /// </summary>
        public static string GenerateValueSchema(Type entityType)
        {
            return GenerateSchema(entityType);
        }

        /// <summary>
        /// AvroEntityConfigurationからバリュースキーマ生成
        /// </summary>
        public static string GenerateValueSchema(Type entityType, AvroEntityConfiguration config)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            var topicName = config.GetEffectiveTopicName();

            var options = new SchemaGenerationOptions
            {
                CustomName = $"{ToPascalCase(topicName)}_value",
                Namespace = $"{entityType.Namespace}.Avro",
                PrettyFormat = false
            };

            return GenerateSchema(entityType, options);
        }

        #endregion

        #region Topic Schema Generation

        /// <summary>
        /// トピック用のキー・バリュースキーマペアを生成
        /// </summary>
        public static (string keySchema, string valueSchema) GenerateTopicSchemas<TKey, TValue>()
        {
            var keySchema = GenerateKeySchema<TKey>();
            var valueSchema = GenerateValueSchema<TValue>();
            return (keySchema, valueSchema);
        }

        /// <summary>
        /// トピック名指定でスキーマペア生成
        /// </summary>
        public static (string keySchema, string valueSchema) GenerateTopicSchemas<TKey, TValue>(string topicName)
        {
            if (string.IsNullOrEmpty(topicName))
                throw new ArgumentException("Topic name cannot be null or empty", nameof(topicName));

            var keySchema = GenerateKeySchema<TKey>();

            var valueType = typeof(TValue);
            var valueOptions = new SchemaGenerationOptions
            {
                CustomName = $"{ToPascalCase(topicName)}_value",
                Namespace = valueType.Namespace ?? "KsqlDsl.Generated"
            };
            var valueSchema = GenerateSchema(valueType, valueOptions);

            return (keySchema, valueSchema);
        }

        /// <summary>
        /// AvroEntityConfigurationからトピックスキーマペア生成
        /// </summary>
        public static (string keySchema, string valueSchema) GenerateTopicSchemas(AvroEntityConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var keySchema = GenerateKeySchema(configuration);
            var valueSchema = GenerateValueSchema(configuration.EntityType, configuration);

            return (keySchema, valueSchema);
        }

        #endregion

        #region Private Helper Methods

        /// <summary>
        /// プリミティブ型かどうかの判定
        /// </summary>
        private static bool IsPrimitiveType(Type type)
        {
            return type == typeof(string) ||
                   type == typeof(int) ||
                   type == typeof(long) ||
                   type == typeof(Guid) ||
                   type == typeof(byte[]) ||
                   type == typeof(bool) ||
                   type == typeof(float) ||
                   type == typeof(double);
        }

        /// <summary>
        /// プリミティブキースキーマ生成
        /// </summary>
        private static string GeneratePrimitiveKeySchema(Type primitiveType)
        {
            return primitiveType switch
            {
                Type t when t == typeof(string) => "\"string\"",
                Type t when t == typeof(int) => "\"int\"",
                Type t when t == typeof(long) => "\"long\"",
                Type t when t == typeof(byte[]) => "\"bytes\"",
                Type t when t == typeof(bool) => "\"boolean\"",
                Type t when t == typeof(float) => "\"float\"",
                Type t when t == typeof(double) => "\"double\"",
                Type t when t == typeof(Guid) => JsonSerializer.Serialize(new
                {
                    type = "string",
                    logicalType = "uuid"
                }),
                _ => "\"string\""
            };
        }

        /// <summary>
        /// Nullable プリミティブキースキーマ生成
        /// </summary>
        private static string GenerateNullablePrimitiveKeySchema(Type primitiveType)
        {
            object innerTypeObj = primitiveType switch
            {
                Type t when t == typeof(string) => "string",
                Type t when t == typeof(int) => "int",
                Type t when t == typeof(long) => "long",
                Type t when t == typeof(byte[]) => "bytes",
                Type t when t == typeof(bool) => "boolean",
                Type t when t == typeof(float) => "float",
                Type t when t == typeof(double) => "double",
                Type t when t == typeof(Guid) => new { type = "string", logicalType = "uuid" },
                _ => "string"
            };

            var unionArray = new object[] { "null", innerTypeObj };
            return JsonSerializer.Serialize(unionArray);
        }

        /// <summary>
        /// 複合キースキーマ生成
        /// </summary>
        private static string GenerateCompositeKeySchema(PropertyInfo[] keyProperties)
        {
            var fields = new List<AvroField>();

            foreach (var prop in keyProperties.OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0))
            {
                fields.Add(new AvroField
                {
                    Name = prop.Name,
                    Type = MapPropertyToAvroType(prop)
                });
            }

            var schema = new AvroSchema
            {
                Type = "record",
                Name = "CompositeKey",
                Fields = fields
            };

            return SerializeSchema(schema);
        }

        /// <summary>
        /// 型からフィールド生成
        /// </summary>
        private static List<AvroField> GenerateFields(Type type)
        {
            var properties = GetSerializableProperties(type);
            var fields = new List<AvroField>();

            foreach (var property in properties)
            {
                var field = new AvroField
                {
                    Name = property.Name,
                    Type = MapPropertyToAvroType(property),
                    Doc = GetPropertyDocumentation(property)
                };

                if (IsNullableProperty(property))
                {
                    field.Default = null;
                }

                fields.Add(field);
            }

            return fields;
        }

        /// <summary>
        /// AvroEntityConfigurationからフィールド生成
        /// </summary>
        private static List<AvroField> GenerateFieldsFromConfiguration(AvroEntityConfiguration configuration)
        {
            var properties = configuration.GetSerializableProperties();
            var fields = new List<AvroField>();

            foreach (var property in properties)
            {
                var field = new AvroField
                {
                    Name = property.Name,
                    Type = MapPropertyToAvroType(property),
                    Doc = GetPropertyDocumentation(property)
                };

                if (IsNullableProperty(property))
                {
                    field.Default = null;
                }

                fields.Add(field);
            }

            return fields;
        }

        /// <summary>
        /// シリアライゼーション対象プロパティ取得
        /// </summary>
        private static PropertyInfo[] GetSerializableProperties(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var allProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            return Array.FindAll(allProperties, p => p.GetCustomAttribute<KafkaIgnoreAttribute>() == null);
        }

        /// <summary>
        /// プロパティをAvro型にマッピング
        /// </summary>
        private static object MapPropertyToAvroType(PropertyInfo property)
        {
            var isNullable = IsNullableProperty(property);
            var avroType = GetAvroType(property);

            return isNullable ? new object[] { "null", avroType } : avroType;
        }

        /// <summary>
        /// Avro型取得
        /// </summary>
        private static object GetAvroType(PropertyInfo property)
        {
            var propertyType = property.PropertyType;
            var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

            // Decimal型の特別処理
            if (underlyingType == typeof(decimal))
            {
                var decimalAttr = property.GetCustomAttribute<DecimalPrecisionAttribute>();
                if (decimalAttr != null)
                {
                    return new
                    {
                        type = "bytes",
                        logicalType = "decimal",
                        precision = decimalAttr.Precision,
                        scale = decimalAttr.Scale
                    };
                }
                return new
                {
                    type = "bytes",
                    logicalType = "decimal",
                    precision = 18,
                    scale = 4
                };
            }

            // DateTime型の特別処理
            if (underlyingType == typeof(DateTime) || underlyingType == typeof(DateTimeOffset))
            {
                var dateTimeAttr = property.GetCustomAttribute<DateTimeFormatAttribute>();
                if (dateTimeAttr?.Format == "date")
                {
                    return new
                    {
                        type = "int",
                        logicalType = "date"
                    };
                }
                return new
                {
                    type = "long",
                    logicalType = "timestamp-millis"
                };
            }

            // Guid型の特別処理
            if (underlyingType == typeof(Guid))
            {
                return new
                {
                    type = "string",
                    logicalType = "uuid"
                };
            }

            // 基本型のマッピング
            return underlyingType switch
            {
                Type t when t == typeof(bool) => "boolean",
                Type t when t == typeof(int) => "int",
                Type t when t == typeof(long) => "long",
                Type t when t == typeof(float) => "float",
                Type t when t == typeof(double) => "double",
                Type t when t == typeof(string) => "string",
                Type t when t == typeof(byte[]) => "bytes",
                Type t when t == typeof(char) => "string",
                Type t when t == typeof(short) => "int",
                Type t when t == typeof(byte) => "int",
                _ => "string"
            };
        }

        /// <summary>
        /// Nullableプロパティかどうかの判定
        /// </summary>
        private static bool IsNullableProperty(PropertyInfo property)
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
                return !propertyType.IsValueType;
            }
        }

        /// <summary>
        /// プロパティドキュメント取得
        /// </summary>
        private static string? GetPropertyDocumentation(PropertyInfo property)
        {
            var kafkaIgnoreAttr = property.GetCustomAttribute<KafkaIgnoreAttribute>();
            if (kafkaIgnoreAttr != null)
                return null;

            // TODO: XML documentation parsing
            return null;
        }

        /// <summary>
        /// PascalCase変換
        /// </summary>
        private static string ToPascalCase(string input)
        {
            if (string.IsNullOrEmpty(input))
                return string.Empty;

            var words = input.Split(new[] { '-', '_', '.' }, StringSplitOptions.RemoveEmptyEntries);
            var result = string.Empty;

            foreach (var word in words)
            {
                if (word.Length > 0)
                {
                    result += char.ToUpperInvariant(word[0]) + (word.Length > 1 ? word.Substring(1).ToLowerInvariant() : string.Empty);
                }
            }

            return result;
        }

        /// <summary>
        /// スキーマシリアライゼーション
        /// </summary>
        private static string SerializeSchema(AvroSchema schema)
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };

            return JsonSerializer.Serialize(schema, options);
        }

        /// <summary>
        /// オプション付きスキーマシリアライゼーション
        /// </summary>
        private static string SerializeSchema(AvroSchema schema, SchemaGenerationOptions options)
        {
            var jsonOptions = new JsonSerializerOptions
            {
                WriteIndented = options.PrettyFormat,
                PropertyNamingPolicy = options.UseKebabCase ? JsonNamingPolicy.KebabCaseLower : JsonNamingPolicy.CamelCase
            };

            return JsonSerializer.Serialize(schema, jsonOptions);
        }

        #endregion

        #region Validation

        /// <summary>
        /// スキーマ検証
        /// </summary>
        public static bool ValidateSchema(string schema)
        {
            if (string.IsNullOrEmpty(schema))
                return false;

            try
            {
                using var document = JsonDocument.Parse(schema);
                var root = document.RootElement;

                if (root.ValueKind == JsonValueKind.Object)
                {
                    if (!root.TryGetProperty("type", out var typeElement))
                        return false;

                    var typeValue = typeElement.GetString();

                    if (typeValue == "record")
                    {
                        if (!root.TryGetProperty("name", out var nameElement))
                            return false;

                        var nameValue = nameElement.GetString();
                        if (string.IsNullOrEmpty(nameValue))
                            return false;
                    }

                    return true;
                }

                if (root.ValueKind == JsonValueKind.String)
                {
                    var primitiveType = root.GetString();
                    return !string.IsNullOrEmpty(primitiveType);
                }

                if (root.ValueKind == JsonValueKind.Array)
                {
                    return root.GetArrayLength() > 0;
                }

                return false;
            }
            catch (JsonException)
            {
                return false;
            }
        }

        /// <summary>
        /// スキーマ生成統計取得
        /// </summary>
        public static SchemaGenerationStats GetGenerationStats(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var allProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var serializableProperties = GetSerializableProperties(type);
            var ignoredProperties = GetIgnoredProperties(type);

            return new SchemaGenerationStats
            {
                TotalProperties = allProperties.Length,
                IncludedProperties = serializableProperties.Length,
                IgnoredProperties = ignoredProperties.Length,
                IgnoredPropertyNames = ignoredProperties.Select(p => p.Name).ToList()
            };
        }

        /// <summary>
        /// 無視されるプロパティ取得
        /// </summary>
        private static PropertyInfo[] GetIgnoredProperties(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var allProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            return Array.FindAll(allProperties, p => p.GetCustomAttribute<KafkaIgnoreAttribute>() != null);
        }

        #endregion
    }
}
