using KsqlDsl.Configuration;
using Microsoft.Extensions.Logging;
using System;

namespace KsqlDsl.Core.Context;

public class KafkaContextOptions
{
    public ILoggerFactory? LoggerFactory { get; set; }
    public bool EnableDebugLogging { get; set; } = false;
    public ValidationMode ValidationMode { get; set; } = ValidationMode.Strict;
    public bool EnableAutoSchemaRegistration { get; set; } = true;
    public Confluent.SchemaRegistry.ISchemaRegistryClient? CustomSchemaRegistryClient { get; set; }
    public Configuration.Abstractions.TopicOverrideService TopicOverrideService { get; set; } = new();

    public void Validate()
    {
        // 基本的な検証
        if (EnableAutoSchemaRegistration && CustomSchemaRegistryClient == null)
        {
            throw new InvalidOperationException("CustomSchemaRegistryClient is required when EnableAutoSchemaRegistration is true");
        }
    }
}