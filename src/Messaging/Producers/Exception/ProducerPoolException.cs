
using KsqlDsl.Core.Exceptions;

namespace KsqlDsl.Messaging.Producers.Exception;

/// <summary>
/// Producerプール例外
/// </summary>
public class ProducerPoolException : KafkaMessageBusException
{
    public ProducerPoolException(string message) : base(message) { }
    public ProducerPoolException(string message, System.Exception innerException) : base(message, innerException) { }
}