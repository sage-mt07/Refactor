using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Validation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Services
{
    public class CoreIntegrationService : ICoreIntegrationService
    {
        private readonly Dictionary<Type, EntityModel> _entityModels = new();

        public async Task<bool> ValidateEntityAsync<T>() where T : class
        {
            await Task.CompletedTask;

            // 基本検証のみ（Serialization層への依存なし）
            return !CoreEntityValidator.HasCircularReference<T>() &&
                   CoreEntityValidator.GetValidProperties<T>().Length > 0;
        }

        public async Task<EntityModel> GetEntityModelAsync<T>() where T : class
        {
            await Task.CompletedTask;

            var entityType = typeof(T);
            if (_entityModels.TryGetValue(entityType, out var model))
                return model;

            // Core層での基本モデル作成
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
            var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

            var entityModel = new EntityModel
            {
                EntityType = entityType,
                TopicAttribute = topicAttribute,
                KeyProperties = keyProperties,
                AllProperties = allProperties
            };

            _entityModels[entityType] = entityModel;
            return entityModel;
        }

        public async Task<CoreHealthReport> GetHealthReportAsync()
        {
            await Task.CompletedTask;

            return new CoreHealthReport
            {
                Status = CoreHealthStatus.Healthy,
                ComponentStatus = new Dictionary<string, object>
                {
                    ["EntityModels"] = _entityModels.Count,
                    ["CoreValidation"] = "Active"
                }
            };
        }

        public CoreDiagnostics GetDiagnostics()
        {
            return new CoreDiagnostics
            {
                Configuration = new Dictionary<string, object>
                {
                    ["LoadedEntities"] = _entityModels.Count,
                    ["ValidationMode"] = "Core"
                },
                Statistics = new Dictionary<string, object>
                {
                    ["TotalModels"] = _entityModels.Count,
                    ["ValidModels"] = _entityModels.Values.Count(m => m.IsValid)
                }
            };
        }
    }
}
