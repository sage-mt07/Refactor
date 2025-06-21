using KsqlDsl.Serialization.Avro.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace KsqlDsl.Serialization.Avro.Management
{
    public class AvroSchemaRepository : IAvroSchemaRepository
    {
        private readonly ConcurrentDictionary<Type, AvroSchemaInfo> _schemasByType = new();
        private readonly ConcurrentDictionary<string, AvroSchemaInfo> _schemasByTopic = new();

        public void StoreSchemaInfo(AvroSchemaInfo schemaInfo)
        {
            if (schemaInfo == null)
                throw new ArgumentNullException(nameof(schemaInfo));

            _schemasByType[schemaInfo.EntityType] = schemaInfo;
            _schemasByTopic[schemaInfo.TopicName] = schemaInfo;
        }

        public AvroSchemaInfo? GetSchemaInfo(Type entityType)
        {
            _schemasByType.TryGetValue(entityType, out var schemaInfo);
            return schemaInfo;
        }

        public AvroSchemaInfo? GetSchemaInfoByTopic(string topicName)
        {
            if (string.IsNullOrEmpty(topicName))
                return null;

            _schemasByTopic.TryGetValue(topicName, out var schemaInfo);
            return schemaInfo;
        }

        public List<AvroSchemaInfo> GetAllSchemas()
        {
            return _schemasByType.Values.ToList();
        }

        public bool IsRegistered(Type entityType)
        {
            return _schemasByType.ContainsKey(entityType);
        }

        public void Clear()
        {
            _schemasByType.Clear();
            _schemasByTopic.Clear();
        }
    }
}
