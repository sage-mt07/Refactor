using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
