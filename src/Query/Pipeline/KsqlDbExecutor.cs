using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using KsqlDsl.Core.Extensions;
namespace KsqlDsl.Query.Pipeline
{
    public abstract class KsqlDbExecutor : IDisposable
    {
        protected readonly ILogger _logger;

        protected KsqlDbExecutor(ILoggerFactory? loggerFactory)
        {
            _logger = loggerFactory.CreateLoggerOrNull<KsqlDbExecutor>();
        }

        public abstract void ExecuteDDL(string ddlQuery);
        public abstract Task ExecuteDDLAsync(string ddlQuery);
        public abstract Task<List<T>> ExecutePullQueryAsync<T>(string query) where T : class;
        public abstract Task<List<T>> ExecutePushQueryAsync<T>(string query) where T : class;
        public abstract Task StopAllQueriesAsync();
        public abstract void Dispose();
    }
}
