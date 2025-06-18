using System;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Abstractions
{
    public interface IAvroSerializationManager<T> : IDisposable where T : class
    {
        Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default);
        Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default);
        Task<bool> ValidateRoundTripAsync(T entity, CancellationToken cancellationToken = default);
        SerializationStatistics GetStatistics();
        Type EntityType { get; }

    }
}