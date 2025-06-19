using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Tracing
{
    public static class AvroActivitySource
    {
        private static readonly ActivitySource _activitySource = new("KsqlDsl.Serialization.Avro", "1.0.0");

        public static Activity? StartSchemaRegistration(string subject)
        {
            return _activitySource.StartActivity("avro.schema.register")
                ?.SetTag("schema.subject", subject);
        }

        public static Activity? StartCacheOperation(string operation, string entityType)
        {
            return _activitySource.StartActivity($"avro.cache.{operation}")
                ?.SetTag("cache.operation", operation)
                ?.SetTag("entity.type", entityType);
        }

        public static void Dispose()
        {
            _activitySource.Dispose();
        }
    }
}
