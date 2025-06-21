using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Exceptions
{
    public class AvroSchemaRegistrationException : Exception
    {
        public AvroSchemaRegistrationException(string message) : base(message) { }
        public AvroSchemaRegistrationException(string message, Exception innerException) : base(message, innerException) { }
    }
}
