using KsqlDsl.Serialization.Abstractions;
using KsqlDsl.Serialization.Avro.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Management
{
    public interface IAvroSchemaRegistrationService
    {
        Task RegisterAllSchemasAsync(IReadOnlyDictionary<Type, AvroEntityConfiguration> configurations);
        Task<AvroSchemaInfo> GetSchemaInfoAsync<T>() where T : class;
        Task<List<AvroSchemaInfo>> GetAllRegisteredSchemasAsync();
    }
}
