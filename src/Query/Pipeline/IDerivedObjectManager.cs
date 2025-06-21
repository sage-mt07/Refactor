using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Query.Pipeline
{
    public interface IDerivedObjectManager
    {
        string CreateDerivedStream(string baseName, Expression linqExpression);
        string CreateDerivedTable(string baseName, Expression linqExpression);
        Task<string> CreateDerivedStreamAsync(string baseName, Expression linqExpression);
        Task<string> CreateDerivedTableAsync(string baseName, Expression linqExpression);
        void CleanupDerivedObjects();
        Task CleanupDerivedObjectsAsync();
    }
}
