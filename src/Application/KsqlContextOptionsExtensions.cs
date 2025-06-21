using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Application
{
    public static class KsqlContextOptionsExtensions
    {
        public static KsqlContextOptions UseSchemaRegistry(
            this KsqlContextOptions options,
            string url)
        {
            var config = new ConfluentSchemaRegistry.SchemaRegistryConfig { Url = url };
            options.SchemaRegistryClient = new ConfluentSchemaRegistry.CachedSchemaRegistryClient(config);
            return options;
        }

        public static KsqlContextOptions UseSchemaRegistry(
            this KsqlContextOptions options,
            ConfluentSchemaRegistry.SchemaRegistryConfig config)
        {
            options.SchemaRegistryClient = new ConfluentSchemaRegistry.CachedSchemaRegistryClient(config);
            return options;
        }

        public static KsqlContextOptions EnableLogging(
            this KsqlContextOptions options,
            ILoggerFactory loggerFactory)
        {
            options.LoggerFactory = loggerFactory;
            return options;
        }

        public static KsqlContextOptions ConfigureValidation(
            this KsqlContextOptions options,
            bool autoRegister = true,
            bool failOnErrors = true,
            bool enablePreWarming = true)
        {
            options.AutoRegisterSchemas = autoRegister;
            options.FailOnInitializationErrors = failOnErrors;
            options.FailOnSchemaErrors = failOnErrors;
            options.EnableCachePreWarming = enablePreWarming;
            return options;
        }

        public static KsqlContextOptions WithTimeouts(
            this KsqlContextOptions options,
            TimeSpan schemaRegistrationTimeout)
        {
            options.SchemaRegistrationTimeout = schemaRegistrationTimeout;
            return options;
        }

        public static KsqlContextOptions EnableDebugMode(
            this KsqlContextOptions options,
            bool enable = true)
        {
            options.EnableDebugLogging = enable;
            return options;
        }
    }
}
