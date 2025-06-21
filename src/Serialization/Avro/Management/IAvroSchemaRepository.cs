using KsqlDsl.Serialization.Avro.Core;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Serialization.Avro.Management
{
    public interface IAvroSchemaRepository
    {
        void StoreSchemaInfo(AvroSchemaInfo schemaInfo);
        AvroSchemaInfo? GetSchemaInfo(Type entityType);
        AvroSchemaInfo? GetSchemaInfoByTopic(string topicName);
        List<AvroSchemaInfo> GetAllSchemas();
        bool IsRegistered(Type entityType);
        void Clear();
    }
}
