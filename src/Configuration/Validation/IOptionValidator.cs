using System.Collections.Generic;
using System.Linq;

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

/// <summary>
/// バリデーション結果
/// </summary>
//public record ValidationResult
//{
//    public bool IsValid { get; init; }
//    public List<string> Errors { get; init; } = new();
//    public List<string> Warnings { get; init; } = new();

//    public static ValidationResult Success() => new() { IsValid = true };

//    public static ValidationResult Failure(params string[] errors) => new()
//    {
//        IsValid = false,
//        Errors = errors.ToList()
//    };

//    public static ValidationResult Warning(params string[] warnings) => new()
//    {
//        IsValid = true,
//        Warnings = warnings.ToList()
//    };
//}