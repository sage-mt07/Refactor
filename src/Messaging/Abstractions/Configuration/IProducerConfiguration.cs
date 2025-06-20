using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Abstractions.Configuration
{
    internal interface IProducerConfiguration
    {
        string Acks { get; }
        string CompressionType { get; }
        bool? EnableIdempotence { get; }
        IReadOnlyDictionary<string, string> AdditionalProperties { get; }
    }
}
