using System;

namespace KsqlDsl.Core.Exceptions;

// =============================================================================
// Exception Classes - 例外クラス
// =============================================================================

/// <summary>
/// KafkaMessageBus基底例外
/// </summary>
public class KafkaMessageBusException : Exception
{
    public KafkaMessageBusException(string message) : base(message) { }
    public KafkaMessageBusException(string message, Exception innerException) : base(message, innerException) { }
}
