using KsqlDsl.Configuration.Options;

namespace KsqlDsl.Monitoring.Diagnostics
{
    public class KafkaConfigurationSnapshot
    {
        public KafkaBusOptions Bus { get; set; } = new();
        public KafkaProducerOptions Producer { get; set; } = new();
        public KafkaConsumerOptions Consumer { get; set; } = new();
        public RetryOptions Retry { get; set; } = new();
        public AvroSchemaRegistryOptions AvroRegistry { get; set; } = new();
    }
}
