using System;
using System.Threading.Tasks;
using KsqlDsl.Configuration.Validation;
namespace KsqlDsl.Configuration.Abstractions;

/// <summary>
/// KsqlDsl設定管理用インターフェース
/// </summary>
public interface IKsqlConfigurationManager
{
    /// <summary>
    /// 指定された型の設定を取得
    /// </summary>
    T GetOptions<T>() where T : class, new();

    /// <summary>
    /// 設定を再読み込み
    /// </summary>
    Task ReloadAsync();

    /// <summary>
    /// 設定変更イベント
    /// </summary>
    event EventHandler<ConfigurationChangedEventArgs> ConfigurationChanged;

    /// <summary>
    /// すべての設定をバリデーション
    /// </summary>
    ValidationResult ValidateAll();
}

public class ConfigurationChangedEventArgs : EventArgs
{
    public string OptionsType { get; }
    public object NewOptions { get; }

    public ConfigurationChangedEventArgs(string optionsType, object newOptions)
    {
        OptionsType = optionsType;
        NewOptions = newOptions;
    }
}
