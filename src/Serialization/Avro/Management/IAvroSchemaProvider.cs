using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Management
{
    public interface IAvroSchemaProvider
    {
        Task<string> GetKeySchemaAsync<T>() where T : class;
        Task<string> GetValueSchemaAsync<T>() where T : class;
        Task<(string keySchema, string valueSchema)> GetSchemasAsync<T>() where T : class;
        Task<bool> ValidateSchemaAsync(string schema);
    }
}
