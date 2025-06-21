using KsqlDsl.Serialization.Abstractions;
using System;

namespace KsqlDsl.Serialization.Avro.Core
{
    public static class AvroSchemaGenerator
    {
        public static string GenerateKeySchema(Type entityType, AvroEntityConfiguration config) =>
            UnifiedSchemaGenerator.GenerateKeySchema(entityType, config);

        public static string GenerateValueSchema(Type entityType, AvroEntityConfiguration config) =>
            UnifiedSchemaGenerator.GenerateValueSchema(entityType, config);
    }
}