using System.Collections.Generic;

namespace KsqlDsl.Messaging.Abstractions.Configuration
{
    internal interface ISchemaRegistryConfiguration
    {
        string Url { get; }
        string BasicAuthUsername { get; }
        string BasicAuthPassword { get; }
        bool? AutoRegisterSchemas { get; }
        int? MaxCachedSchemas { get; }
        IReadOnlyDictionary<string, string> AdditionalProperties { get; }
    }
}
