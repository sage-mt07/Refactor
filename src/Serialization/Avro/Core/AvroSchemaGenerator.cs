using KsqlDsl.Serialization.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Core
{
    public static class AvroSchemaGenerator
    {
        public static string GenerateKeySchema(Type entityType, AvroEntityConfiguration config)
        {
            if (config.KeyProperties == null || config.KeyProperties.Length == 0)
                return SchemaGenerator.GenerateKeySchema<string>();

            if (config.KeyProperties.Length == 1)
                return SchemaGenerator.GenerateKeySchema(config.KeyProperties[0].PropertyType);

            return SchemaGenerator.GenerateKeySchema(typeof(System.Collections.Generic.Dictionary<string, object>));
        }

        public static string GenerateValueSchema(Type entityType, AvroEntityConfiguration config)
        {
            return SchemaGenerator.GenerateSchema(entityType, new SchemaGenerationOptions
            {
                CustomName = $"{config.TopicName ?? entityType.Name}_value",
                Namespace = $"{entityType.Namespace}.Avro",
                PrettyFormat = false
            });
        }
    }
}
