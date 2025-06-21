using System;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions;

/// <summary>
/// シリアライザ共通インターフェース
/// Avro/JSON/Protobuf対応
/// </summary>
public interface ISerializationManager<T> : IDisposable where T : class
{
    Task<SerializerConfiguration<T>> GetConfigurationAsync();
    Task<bool> ValidateAsync(T entity);


    Type EntityType { get; }

}


