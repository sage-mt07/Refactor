using KsqlDsl.Core.Exceptions;
using System;

namespace KsqlDsl.Messaging.Consumers.Exceptions;

/// <summary>
/// Consumerプール例外
/// </summary>
public class ConsumerPoolException : KafkaMessageBusException
{
    public ConsumerPoolException(string message) : base(message) { }
    public ConsumerPoolException(string message, Exception innerException) : base(message, innerException) { }
}