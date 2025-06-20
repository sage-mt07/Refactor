using System.Collections.Generic;

namespace KsqlDsl.Messaging.Abstractions.Configuration
{
    internal interface IConsumerConfiguration
    {
        string GroupId { get; }
        string AutoOffsetReset { get; }
        bool? EnableAutoCommit { get; }
        IReadOnlyDictionary<string, string> AdditionalProperties { get; }
    }
}
