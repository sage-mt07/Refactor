using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers.Exceptions
{
    public class KafkaConsumerException : Exception
    {
        public KafkaConsumerException(string message) : base(message) { }
        public KafkaConsumerException(string message, Exception innerException) : base(message, innerException) { }
    }
}
