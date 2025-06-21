using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;
namespace KsqlDsl.Application
{
    public class KsqlContextOptions
    {
        public ISchemaRegistryClient SchemaRegistryClient { get; set; } = null!;
        public ILoggerFactory? LoggerFactory { get; set; }
        public bool EnableDebugLogging { get; set; } = false;
        public bool AutoRegisterSchemas { get; set; } = true;
        public bool EnableCachePreWarming { get; set; } = true;
        public bool FailOnInitializationErrors { get; set; } = true;
        public bool FailOnSchemaErrors { get; set; } = true;
        public TimeSpan SchemaRegistrationTimeout { get; set; } = TimeSpan.FromSeconds(30);

        public void Validate()
        {
            if (SchemaRegistryClient == null)
                throw new InvalidOperationException("SchemaRegistryClient is required");

            if (SchemaRegistrationTimeout <= TimeSpan.Zero)
                throw new InvalidOperationException("SchemaRegistrationTimeout must be positive");
        }
    }
}
