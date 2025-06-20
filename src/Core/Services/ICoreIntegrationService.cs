﻿using KsqlDsl.Core.Abstractions;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Services
{
    public interface ICoreIntegrationService
    {
        Task<bool> ValidateEntityAsync<T>() where T : class;
        Task<EntityModel> GetEntityModelAsync<T>() where T : class;
        Task<CoreHealthReport> GetHealthReportAsync();
        CoreDiagnostics GetDiagnostics();
    }
}
