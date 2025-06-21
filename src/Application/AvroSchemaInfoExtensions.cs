using KsqlDsl.Serialization.Avro.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Application
{
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

        public static string GetStreamTableType(this AvroSchemaInfo schemaInfo)
        {
            return schemaInfo.HasCustomKey ? "Table" : "Stream";
        }

        public static string GetKeyTypeName(this AvroSchemaInfo schemaInfo)
        {
            if (!schemaInfo.HasCustomKey)
                return "string";

            if (schemaInfo.KeyProperties?.Length == 1)
                return schemaInfo.KeyProperties[0].PropertyType.Name;

            return "CompositeKey";
        }

        public static string GetSummary(this AvroSchemaInfo schemaInfo)
        {
            var keyInfo = schemaInfo.IsCompositeKey() ? "CompositeKey" : schemaInfo.GetKeyTypeName();
            return $"{schemaInfo.EntityType.Name} → {schemaInfo.TopicName} ({schemaInfo.GetStreamTableType()}, Key: {keyInfo})";
        }
    }
}
