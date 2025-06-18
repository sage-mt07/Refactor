using System;

namespace KsqlDsl.Messaging.Consumers.Exceptions
{
    public class KafkaConsumerException : Exception
    {
        public KafkaConsumerException(string message) : base(message) { }
        public KafkaConsumerException(string message, Exception innerException) : base(message, innerException) { }
    }
}
