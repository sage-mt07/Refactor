namespace KsqlDsl.Serialization.Avro.Adapters;

using Confluent.Kafka;
using KsqlDsl.Serialization.Avro.Abstractions;
using System;
using System.Runtime.Serialization;

/// <summary>
/// IAvroDeserializer<T>をConfluent.Kafka.IDeserializer<object>に適合させるアダプター
/// </summary>
public class AvroDeserializerAdapter<T> : IDeserializer<object> where T : class
{
    private readonly IAvroDeserializer<T> _avroDeserializer;

    public AvroDeserializerAdapter(IAvroDeserializer<T> avroDeserializer)
    {
        _avroDeserializer = avroDeserializer ?? throw new ArgumentNullException(nameof(avroDeserializer));
    }

    public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return null!;

        try
        {
            // IAvroDeserializer<T>のDeserializeメソッドを呼び出し
            var result = _avroDeserializer.Deserialize(data.ToArray());
            return result;
        }
        catch (Exception ex)
        {
            throw new SerializationException($"Failed to deserialize Avro data to type {typeof(T).Name}", ex);
        }
    }

    public void Dispose()
    {
        _avroDeserializer?.Dispose();
    }
}
