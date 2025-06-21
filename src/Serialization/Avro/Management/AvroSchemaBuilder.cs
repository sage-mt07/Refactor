using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Management
{
    public class AvroSchemaBuilder : IAvroSchemaProvider
    {
        public Task<string> GetKeySchemaAsync<T>() where T : class
        {
            var keySchema = GenerateKeySchema<T>();
            return Task.FromResult(keySchema);
        }

        public Task<string> GetValueSchemaAsync<T>() where T : class
        {
            var valueSchema = GenerateValueSchema<T>();
            return Task.FromResult(valueSchema);
        }

        public Task<(string keySchema, string valueSchema)> GetSchemasAsync<T>() where T : class
        {
            var keySchema = GenerateKeySchema<T>();
            var valueSchema = GenerateValueSchema<T>();
            return Task.FromResult((keySchema, valueSchema));
        }

        public Task<bool> ValidateSchemaAsync(string schema)
        {
            var isValid = ValidateAvroSchema(schema);
            return Task.FromResult(isValid);
        }

        public string GenerateKeySchema<T>() where T : class
        {
            var entityType = typeof(T);
            var keyProperties = GetKeyProperties(entityType);

            if (keyProperties.Length == 0)
                return GeneratePrimitiveSchema(typeof(string));

            if (keyProperties.Length == 1)
                return GeneratePrimitiveSchema(keyProperties[0].PropertyType);

            return GenerateCompositeKeySchema(keyProperties);
        }

        public string GenerateValueSchema<T>() where T : class
        {
            var entityType = typeof(T);
            var topicName = GetTopicName(entityType);

            var schema = new AvroSchema
            {
                Type = "record",
                Name = $"{topicName}_value",
                Namespace = $"{entityType.Namespace}.Avro",
                Fields = GenerateFieldsFromProperties(GetSchemaProperties(entityType))
            };

            return SerializeSchema(schema);
        }

        private string GeneratePrimitiveSchema(Type primitiveType)
        {
            var underlyingType = Nullable.GetUnderlyingType(primitiveType) ?? primitiveType;

            if (underlyingType == typeof(decimal))
            {
                return JsonSerializer.Serialize(new
                {
                    type = "bytes",
                    logicalType = "decimal",
                    precision = 18,
                    scale = 4
                });
            }

            if (underlyingType == typeof(DateTime) || underlyingType == typeof(DateTimeOffset))
            {
                return JsonSerializer.Serialize(new
                {
                    type = "long",
                    logicalType = "timestamp-millis"
                });
            }

            if (underlyingType == typeof(Guid))
            {
                return JsonSerializer.Serialize(new
                {
                    type = "string",
                    logicalType = "uuid"
                });
            }

            return underlyingType switch
            {
                Type t when t == typeof(string) => "\"string\"",
                Type t when t == typeof(int) => "\"int\"",
                Type t when t == typeof(long) => "\"long\"",
                Type t when t == typeof(byte[]) => "\"bytes\"",
                _ => "\"string\""
            };
        }

        private string GenerateCompositeKeySchema(PropertyInfo[] keyProperties)
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

        private List<AvroField> GenerateFieldsFromProperties(PropertyInfo[] properties)
        {
            var fields = new List<AvroField>();

            foreach (var property in properties)
            {
                if (property.GetCustomAttribute<KafkaIgnoreAttribute>() != null)
                    continue;

                fields.Add(new AvroField
                {
                    Name = property.Name,
                    Type = MapPropertyToAvroType(property)
                });
            }

            return fields;
        }

        private object MapPropertyToAvroType(PropertyInfo property)
        {
            var propertyType = property.PropertyType;
            var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
            var isNullable = Nullable.GetUnderlyingType(propertyType) != null ||
                           (!propertyType.IsValueType && IsNullableReferenceType(property));

            var avroType = GetBasicAvroType(property, underlyingType);

            return isNullable ? new object[] { "null", avroType } : avroType;
        }

        private object GetBasicAvroType(PropertyInfo property, Type underlyingType)
        {
            if (underlyingType == typeof(decimal))
            {
                var decimalAttr = property.GetCustomAttribute<DecimalPrecisionAttribute>();
                return new
                {
                    type = "bytes",
                    logicalType = "decimal",
                    precision = decimalAttr?.Precision ?? 18,
                    scale = decimalAttr?.Scale ?? 4
                };
            }

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

            if (underlyingType == typeof(Guid))
            {
                return new
                {
                    type = "string",
                    logicalType = "uuid"
                };
            }

            return underlyingType switch
            {
                Type t when t == typeof(bool) => "boolean",
                Type t when t == typeof(int) => "int",
                Type t when t == typeof(long) => "long",
                Type t when t == typeof(float) => "float",
                Type t when t == typeof(double) => "double",
                Type t when t == typeof(string) => "string",
                Type t when t == typeof(byte[]) => "bytes",
                _ => "string"
            };
        }

        private bool IsNullableReferenceType(PropertyInfo property)
        {
            try
            {
                var nullabilityContext = new NullabilityInfoContext();
                var nullabilityInfo = nullabilityContext.Create(property);
                return nullabilityInfo.WriteState == NullabilityState.Nullable;
            }
            catch
            {
                return !property.PropertyType.IsValueType;
            }
        }

        private PropertyInfo[] GetKeyProperties(Type entityType)
        {
            var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

            Array.Sort(keyProperties, (p1, p2) =>
            {
                var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
                return order1.CompareTo(order2);
            });

            return keyProperties;
        }

        private PropertyInfo[] GetSchemaProperties(Type entityType)
        {
            var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            return Array.FindAll(allProperties, p => p.GetCustomAttribute<KafkaIgnoreAttribute>() == null);
        }

        private string GetTopicName(Type entityType)
        {
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            return topicAttribute?.TopicName ?? entityType.Name;
        }

        private string SerializeSchema(AvroSchema schema)
        {
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };

            return JsonSerializer.Serialize(schema, options);
        }

        private bool ValidateAvroSchema(string schema)
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
    }
}