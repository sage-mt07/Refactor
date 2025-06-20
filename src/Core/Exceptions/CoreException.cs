using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Exceptions
{
    public abstract class CoreException : Exception
    {
        protected CoreException(string message) : base(message) { }
        protected CoreException(string message, Exception innerException) : base(message, innerException) { }
    }
}
