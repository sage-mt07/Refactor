namespace KsqlDsl.Serialization.Avro.Abstractions;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Avroシリアライザーのインターフェース
/// </summary>
public interface IAvroSerializer<T> : IDisposable where T : class
{
    /// <summary>
    /// オブジェクトをAvroバイト配列にシリアライズ
    /// </summary>
    byte[] Serialize(T value);

    /// <summary>
    /// オブジェクトをストリームにシリアライズ
    /// </summary>
    void Serialize(T value, Stream stream);

    /// <summary>
    /// オブジェクトを非同期でストリームにシリアライズ
    /// </summary>
    Task SerializeAsync(T value, Stream stream, CancellationToken cancellationToken = default);
}
