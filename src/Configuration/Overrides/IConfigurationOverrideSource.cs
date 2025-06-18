using System;
using System.Collections.Generic;

namespace KsqlDsl.Configuration.Overrides;

/// <summary>
/// 設定上書きソースのインターフェース
/// </summary>
public interface IConfigurationOverrideSource
{
    /// <summary>
    /// 上書き優先順位（低い値ほど高優先度）
    /// </summary>
    int Priority { get; }

    /// <summary>
    /// 設定値を取得
    /// </summary>
    string? GetValue(string key);

    /// <summary>
    /// 指定されたプレフィックスで始まる全設定を取得
    /// </summary>
    Dictionary<string, string> GetValues(string prefix);

    /// <summary>
    /// 設定変更の監視を開始
    /// </summary>
    void StartWatching(Action<string, string?> onChanged);

    /// <summary>
    /// 設定変更の監視を停止
    /// </summary>
    void StopWatching();
}