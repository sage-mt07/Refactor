using KsqlDsl.Configuration.Overrides;
using KsqlDsl.Configuration.Validation;
using System;

namespace KsqlDsl.Configuration.Abstractions;

/// <summary>
/// 設定ビルダーインターフェース
/// </summary>
public interface IOptionsBuilder<T> where T : class
{
    /// <summary>
    /// 設定値を指定
    /// </summary>
    IOptionsBuilder<T> Configure(Action<T> configureOptions);

    /// <summary>
    /// バリデーターを追加
    /// </summary>
    IOptionsBuilder<T> AddValidator(IOptionValidator<T> validator);

    /// <summary>
    /// 上書きソースを追加
    /// </summary>
    IOptionsBuilder<T> AddOverrideSource(IConfigurationOverrideSource source);

    /// <summary>
    /// 設定を構築
    /// </summary>
    T Build();
}