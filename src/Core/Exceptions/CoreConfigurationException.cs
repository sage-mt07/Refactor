using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Exceptions
{
    public class CoreConfigurationException : CoreException
    {
        public CoreConfigurationException(string message) : base(message) { }
        public CoreConfigurationException(string message, Exception innerException) : base(message, innerException) { }
    }
}
