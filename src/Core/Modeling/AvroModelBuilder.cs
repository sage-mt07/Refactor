using KsqlDsl.Application;
using KsqlDsl.Serialization.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Modeling
{
    public class AvroModelBuilder
    {
        private readonly Dictionary<Type, AvroEntityConfiguration> _configurations = new();
        private readonly ILogger<AvroModelBuilder>? _logger;

        public AvroModelBuilder(ILoggerFactory? loggerFactory = null)
        {
            _logger = loggerFactory?.CreateLogger<AvroModelBuilder>() ?? NullLogger<AvroModelBuilder>.Instance;
        }

        /// <summary>
        /// エンティティの設定を開始
        /// </summary>
        public AvroEntityTypeBuilder<T> Entity<T>() where T : class
        {
            var entityType = typeof(T);

            if (!_configurations.ContainsKey(entityType))
            {
                _configurations[entityType] = new AvroEntityConfiguration(entityType);
                _logger?.LogDebug("Added entity configuration for {EntityType}", entityType.Name);
            }

            return new AvroEntityTypeBuilder<T>(_configurations[entityType], this);
        }

        /// <summary>
        /// エンティティが既に設定されているかチェック
        /// </summary>
        public bool HasEntity<T>() where T : class
        {
            return _configurations.ContainsKey(typeof(T));
        }

        /// <summary>
        /// エンティティが既に設定されているかチェック（型指定）
        /// </summary>
        public bool HasEntity(Type entityType)
        {
            return _configurations.ContainsKey(entityType);
        }

        /// <summary>
        /// 設定からエンティティを削除
        /// </summary>
        public bool RemoveEntity<T>() where T : class
        {
            var entityType = typeof(T);
            var removed = _configurations.Remove(entityType);

            if (removed)
            {
                _logger?.LogDebug("Removed entity configuration for {EntityType}", entityType.Name);
            }

            return removed;
        }

        /// <summary>
        /// 全設定をクリア
        /// </summary>
        public void Clear()
        {
            var count = _configurations.Count;
            _configurations.Clear();
            _logger?.LogDebug("Cleared all entity configurations ({Count} entities)", count);
        }

        /// <summary>
        /// 設定をビルドして取得
        /// </summary>
        public IReadOnlyDictionary<Type, AvroEntityConfiguration> Build()
        {
            ValidateAllConfigurations();
            _logger?.LogInformation("Built model with {EntityCount} entities", _configurations.Count);
            return _configurations.AsReadOnly();
        }

        /// <summary>
        /// 設定されているエンティティ数
        /// </summary>
        public int EntityCount => _configurations.Count;

        /// <summary>
        /// 特定のエンティティ設定を取得
        /// </summary>
        public AvroEntityConfiguration? GetEntityConfiguration<T>() where T : class
        {
            return GetEntityConfiguration(typeof(T));
        }

        /// <summary>
        /// 特定のエンティティ設定を取得（型指定）
        /// </summary>
        public AvroEntityConfiguration? GetEntityConfiguration(Type entityType)
        {
            _configurations.TryGetValue(entityType, out var configuration);
            return configuration;
        }

        /// <summary>
        /// モデルサマリーを取得
        /// </summary>
        public string GetModelSummary()
        {
            if (_configurations.Count == 0)
                return "AvroModelBuilder: No entities configured";

            var summary = new List<string>
            {
                $"AvroModelBuilder: {_configurations.Count} entities configured",
                ""
            };

            foreach (var (entityType, config) in _configurations.OrderBy(x => x.Key.Name))
            {
                var validation = config.Validate();
                var status = validation.IsValid ? "✅" : "❌";
                summary.Add($"{status} {config.GetSummary()}");

                if (!validation.IsValid)
                {
                    foreach (var error in validation.Errors)
                    {
                        summary.Add($"   Error: {error}");
                    }
                }

                foreach (var warning in validation.Warnings)
                {
                    summary.Add($"   Warning: {warning}");
                }
            }

            return string.Join(Environment.NewLine, summary);
        }

        /// <summary>
        /// 全設定の検証
        /// </summary>
        public ModelValidationResult ValidateModel()
        {
            var result = new ModelValidationResult();

            foreach (var (entityType, config) in _configurations)
            {
                var validation = config.Validate();

                if (!validation.IsValid)
                {
                    result.HasErrors = true;
                    result.EntityErrors[entityType] = validation.Errors;
                }

                if (validation.Warnings.Count > 0)
                {
                    result.EntityWarnings[entityType] = validation.Warnings;
                }
            }

            // トピック名の重複チェック
            CheckTopicNameDuplicates(result);

            return result;
        }

        /// <summary>
        /// 属性ベースの自動設定を適用
        /// </summary>
        public AvroModelBuilder ApplyAttributeBasedConfiguration()
        {
            foreach (var (entityType, config) in _configurations.ToList())
            {
                // 属性情報は既にAvroEntityConfigurationのコンストラクタで適用済み
                // ここでは追加の検証や調整を行う

                _logger?.LogDebug("Applied attribute-based configuration for {EntityType}", entityType.Name);
            }

            return this;
        }

        /// <summary>
        /// バッチ設定適用
        /// </summary>
        public AvroModelBuilder ApplyConvention(Action<AvroModelBuilder> convention)
        {
            if (convention == null)
                throw new ArgumentNullException(nameof(convention));

            convention(this);
            return this;
        }

        #region Private Methods

        /// <summary>
        /// 全設定の検証
        /// </summary>
        private void ValidateAllConfigurations()
        {
            var validationResult = ValidateModel();

            if (validationResult.HasErrors)
            {
                var errorSummary = string.Join(Environment.NewLine,
                    validationResult.EntityErrors.SelectMany(x =>
                        x.Value.Select(error => $"{x.Key.Name}: {error}")));

                throw new InvalidOperationException($"Model validation failed:{Environment.NewLine}{errorSummary}");
            }

            if (validationResult.EntityWarnings.Any())
            {
                var warningSummary = string.Join(Environment.NewLine,
                    validationResult.EntityWarnings.SelectMany(x =>
                        x.Value.Select(warning => $"{x.Key.Name}: {warning}")));

                _logger?.LogWarning("Model validation warnings:{NewLine}{Warnings}", Environment.NewLine, warningSummary);
            }
        }

        /// <summary>
        /// トピック名重複チェック
        /// </summary>
        private void CheckTopicNameDuplicates(ModelValidationResult result)
        {
            var topicGroups = _configurations
                .GroupBy(x => x.Value.GetEffectiveTopicName())
                .Where(g => g.Count() > 1)
                .ToList();

            foreach (var group in topicGroups)
            {
                result.HasErrors = true;
                var entityNames = string.Join(", ", group.Select(x => x.Key.Name));
                var error = $"Duplicate topic name '{group.Key}' used by entities: {entityNames}";

                foreach (var item in group)
                {
                    if (!result.EntityErrors.ContainsKey(item.Key))
                    {
                        result.EntityErrors[item.Key] = new List<string>();
                    }
                    result.EntityErrors[item.Key].Add(error);
                }
            }
        }

        #endregion
    }
}
