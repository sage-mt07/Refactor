using KsqlDsl.Configuration;

namespace KsqlDsl.Core.Context;

public class KafkaContextOptions
{

    public ValidationMode ValidationMode { get; set; } = ValidationMode.Strict;

}