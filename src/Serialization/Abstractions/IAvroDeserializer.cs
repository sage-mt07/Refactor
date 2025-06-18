namespace KsqlDsl.Serialization.Avro.Abstractions;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Avroデシリアライザーのインターフェース
/// </summary>
public interface IAvroDeserializer<T> : IDisposable where T : class
{
    /// <summary>
    /// Avroバイト配列からオブジェクトにデシリアライズ
    /// </summary>
    T Deserialize(byte[] data);

    /// <summary>
    /// ReadOnlySpanからオブジェクトにデシリアライズ
    /// </summary>
    T Deserialize(ReadOnlySpan<byte> data);

    /// <summary>
    /// ストリームから非同期でオブジェクトにデシリアライズ
    /// </summary>
    Task<T> DeserializeAsync(Stream stream, CancellationToken cancellationToken = default);
}