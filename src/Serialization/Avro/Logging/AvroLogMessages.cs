using Microsoft.Extensions.Logging;
using System;

namespace KsqlDsl.Serialization.Avro.Logging
{
    public static class AvroLogMessages
    {
        private static readonly Action<ILogger, string, string, int, long, long, Exception?> _slowSerializerCreation =
            LoggerMessage.Define<string, string, int, long, long>(
                LogLevel.Warning,
                new EventId(1001, nameof(SlowSerializerCreation)),
                "Slow serializer creation: Entity={EntityType}, Type={SerializerType}, SchemaId={SchemaId}, Duration={Duration}ms, Threshold={Threshold}ms");

        public static void SlowSerializerCreation(ILogger logger, string entityType, string serializerType, int schemaId, long duration, long threshold)
        {
            _slowSerializerCreation(logger, entityType, serializerType, schemaId, duration, threshold, null);
        }
    }
}
