using System;

namespace KsqlDsl.Core.Exceptions
{
    public abstract class CoreException : Exception
    {
        protected CoreException(string message) : base(message) { }
        protected CoreException(string message, Exception innerException) : base(message, innerException) { }
    }
}
