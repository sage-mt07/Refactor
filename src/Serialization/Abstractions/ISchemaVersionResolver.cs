using KsqlDsl.Serialization.Avro.Management;
using System.Collections.Generic;
using System.Threading.Tasks;
namespace KsqlDsl.Serialization.Abstractions
{
    public interface ISchemaVersionResolver
    {
        Task<int> ResolveKeySchemaVersionAsync<T>() where T : class;
        Task<int> ResolveValueSchemaVersionAsync<T>() where T : class;
        Task<bool> CanUpgradeAsync<T>(string newSchema) where T : class;
        Task<SchemaUpgradeResult> UpgradeAsync<T>() where T : class;
        Task<List<SchemaVersionInfo>> GetSchemaVersionHistoryAsync<T>() where T : class;
        Task<SchemaCompatibilityReport> CheckCompatibilityAsync<T>() where T : class;
        Task<bool> DeleteSchemaVersionAsync<T>(int version) where T : class;
    }
}
