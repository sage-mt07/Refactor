using System;

namespace KsqlDsl.Core.Exceptions
{
    public class CoreConfigurationException : CoreException
    {
        public CoreConfigurationException(string message) : base(message) { }
        public CoreConfigurationException(string message, Exception innerException) : base(message, innerException) { }
    }
}
