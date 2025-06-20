using System.Collections.Generic;

namespace KsqlDsl.Core.Exceptions
{
    public class CoreValidationException : CoreException
    {
        public List<string> ValidationErrors { get; }

        public CoreValidationException(string message, List<string> validationErrors) : base(message)
        {
            ValidationErrors = validationErrors ?? new List<string>();
        }

        public CoreValidationException(List<string> validationErrors)
            : base($"Validation failed with {validationErrors?.Count ?? 0} errors")
        {
            ValidationErrors = validationErrors ?? new List<string>();
        }
    }
}
