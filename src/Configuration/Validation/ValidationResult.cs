using System.Collections.Generic;
using System.Linq;

namespace KsqlDsl.Configuration.Validation;

public class ValidationResult
{
    public bool IsValid { get; set; }

    public List<string> Errors { get; set; } = new();

    public List<string> Warnings { get; set; } = new();

    public List<string> AutoCompletedSettings { get; set; } = new();

    public bool HasIssues => Errors.Any() || Warnings.Any();

    // ✅ 追加: ファクトリメソッド
    public static ValidationResult Success()
    {
        return new ValidationResult { IsValid = true };
    }

    public static ValidationResult Failure(params string[] errors)
    {
        return new ValidationResult
        {
            IsValid = false,
            Errors = errors.ToList()
        };
    }

    public static ValidationResult Failure(string error)
    {
        return new ValidationResult
        {
            IsValid = false,
            Errors = new List<string> { error }
        };
    }
}