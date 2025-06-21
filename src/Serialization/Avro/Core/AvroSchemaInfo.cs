using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Core
{
    public class AvroSchemaInfo
    {
        public Type EntityType { get; set; } = null!;
        public string TopicName { get; set; } = string.Empty;
        public int KeySchemaId { get; set; }
        public int ValueSchemaId { get; set; }
        public string KeySchema { get; set; } = string.Empty;
        public string ValueSchema { get; set; } = string.Empty;
        public DateTime RegisteredAt { get; set; }

        public bool HasCustomKey => KeyProperties != null && KeyProperties.Length > 0;
        public PropertyInfo[]? KeyProperties { get; set; }
        public Type? KeyType => HasCustomKey && KeyProperties!.Length == 1
            ? KeyProperties[0].PropertyType
            : typeof(string);

        public override string ToString()
        {
            return $"Schema: {EntityType.Name} → Topic: {TopicName} (Key: {KeySchemaId}, Value: {ValueSchemaId})";
        }
    }
}
