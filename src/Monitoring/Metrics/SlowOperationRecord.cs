using System;

namespace KsqlDsl.Monitoring.Metrics
{
    public class SlowOperationRecord
    {
        public string EntityTypeName { get; set; } = string.Empty;
        public string OperationType { get; set; } = string.Empty;
        public string SerializerType { get; set; } = string.Empty;
        public TimeSpan Duration { get; set; }
        public DateTime Timestamp { get; set; }
    }

}
