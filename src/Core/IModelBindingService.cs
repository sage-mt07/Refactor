using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Core;

public interface IModelBindingService
{
    EntityModel CreateEntityModel<T>() where T : class;
    EntityModel CreateEntityModel(Type entityType);
    Dictionary<string, object> ExtractModelConfiguration(Type entityType);
    string GetConfigurationSummary(Type entityType);
}
