namespace KsqlDsl.Configuration.Validation;

/// <summary>
/// オプション設定のバリデーションインターフェース
/// </summary>
public interface IOptionValidator<T> where T : class
{
    /// <summary>
    /// 設定をバリデーションし、結果を返す
    /// </summary>
    ValidationResult Validate(T options);

    /// <summary>
    /// 設定をバリデーションし、失敗時に例外をスロー
    /// </summary>
    void ValidateAndThrow(T options);
}
