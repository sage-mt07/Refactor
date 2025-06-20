using KsqlDsl.Core;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Context;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Configuration
{
    public class ModelBindingService : IModelBindingService
    {
        public EntityModel CreateEntityModel<T>() where T : class
        {
            return ModelBinding.CreateEntityModel<T>();
        }

        public EntityModel CreateEntityModel(Type entityType)
        {
            return ModelBinding.CreateEntityModel(entityType);
        }

        public Dictionary<string, object> ExtractModelConfiguration(Type entityType)
        {
            return ModelBinding.ExtractModelConfiguration(entityType);
        }

        public string GetConfigurationSummary(Type entityType)
        {
            return ModelBinding.GetConfigurationSummary(entityType);
        }
    }
}
