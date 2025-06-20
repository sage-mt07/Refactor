using KsqlDsl.Configuration.Abstractions;

namespace KsqlDsl.Configuration.Builders;

public class KafkaContextOptionsBuilder
{
    private readonly KafkaContextOptions _options = new();

    public KafkaContextOptionsBuilder UseConnectionString(string connectionString)
    {
        _options.ConnectionString = connectionString;
        return this;
    }

    public KafkaContextOptionsBuilder UseSchemaRegistry(string url)
    {
        _options.SchemaRegistryUrl = url;
        return this;
    }

    public KafkaContextOptionsBuilder UseValidationMode(ValidationMode mode)
    {
        _options.ValidationMode = mode;
        return this;
    }
    public KafkaContextOptions GetOptions()
    {
        return _options;
    }
    public KafkaContextOptions Build() => _options.Clone();
}