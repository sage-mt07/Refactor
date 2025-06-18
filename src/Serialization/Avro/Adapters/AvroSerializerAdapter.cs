namespace KsqlDsl.Serialization.Avro.Adapters;

using Confluent.Kafka;
using KsqlDsl.Serialization.Avro.Abstractions;
using System;
using System.Runtime.Serialization;

/// <summary>
/// IAvroSerializer<T>をConfluent.Kafka.ISerializer<object>に適合させるアダプター
/// </summary>
public class AvroSerializerAdapter<T> : ISerializer<object> where T : class
{
    private readonly IAvroSerializer<T> _avroSerializer;

    public AvroSerializerAdapter(IAvroSerializer<T> avroSerializer)
    {
        _avroSerializer = avroSerializer ?? throw new ArgumentNullException(nameof(avroSerializer));
    }

    public byte[] Serialize(object data, SerializationContext context)
    {
        if (data == null)
            return null!;

        if (data is not T typedData)
        {
            throw new ArgumentException($"Expected type {typeof(T).Name}, but got {data.GetType().Name}");
        }

        try
        {
            return _avroSerializer.Serialize(typedData);
        }
        catch (Exception ex)
        {
            throw new SerializationException($"Failed to serialize data of type {typeof(T).Name}", ex);
        }
    }

    public void Dispose()
    {
        _avroSerializer?.Dispose();
    }
}