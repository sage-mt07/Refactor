using KsqlDsl.Serialization.Avro.Cache;
using System;

namespace KsqlDsl.Serialization.Avro.Core
{
    public class AvroSchemaInfo
    {
        public Type EntityType { get; set; } = null!;
        public SerializerType Type { get; set; }
        public int SchemaId { get; set; }
        public string Subject { get; set; } = string.Empty;
        public DateTime RegisteredAt { get; set; }
        public DateTime LastUsed { get; set; }
        public long UsageCount { get; set; }
        public int Version { get; set; }
        public string AvroSchema { get; set; } = string.Empty;
    }
}
