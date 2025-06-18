using KsqlDsl.Core.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Producers.Exception;


/// <summary>
/// Producer管理例外
/// </summary>
public class KafkaProducerManagerException : KafkaMessageBusException
{
    public KafkaProducerManagerException(string message) : base(message) { }
    public KafkaProducerManagerException(string message, System.Exception innerException) : base(message, innerException) { }
}