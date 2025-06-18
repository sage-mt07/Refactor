using KsqlDsl.Core.Exceptions;
using KsqlDsl.Messaging.Producers.Core;

namespace KsqlDsl.Messaging.Producers.Exception;


/// <summary>
/// バッチ送信例外
/// </summary>
public class KafkaBatchSendException : KafkaMessageBusException
{
    public KafkaBatchDeliveryResult BatchResult { get; }

    public KafkaBatchSendException(string message, KafkaBatchDeliveryResult batchResult) : base(message)
    {
        BatchResult = batchResult;
    }

    public KafkaBatchSendException(string message, KafkaBatchDeliveryResult batchResult, System.Exception innerException)
        : base(message, innerException)
    {
        BatchResult = batchResult;
    }
}