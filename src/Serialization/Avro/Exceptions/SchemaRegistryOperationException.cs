using System;

namespace KsqlDsl.Serialization.Avro.Exceptions;

public class SchemaRegistryOperationException : Exception
{
    public SchemaRegistryOperationException()
    {
    }

    public SchemaRegistryOperationException(string message) : base(message)
    {
    }

    public SchemaRegistryOperationException(string message, Exception innerException) : base(message, innerException)
    {
    }
}