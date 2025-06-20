using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Configuration.Validation;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Modeling;

public class ModelBuilder
{
    private readonly Dictionary<Type, EntityModel> _entityModels = new();
    private readonly ValidationService _validationService;
    private bool _isBuilt = false;

    public ModelBuilder(ValidationMode validationMode = ValidationMode.Strict)
    {
        _validationService = new ValidationService(validationMode);
    }

    public EntityModelBuilder<T> Event<T>() where T : class
    {
        var entityType = typeof(T);

        if (_entityModels.ContainsKey(entityType))
        {
            throw new InvalidOperationException($"エンティティ {entityType.Name} は既に登録済みです。");
        }

        var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
        var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

        Array.Sort(keyProperties, (p1, p2) =>
        {
            var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
            var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
            return order1.CompareTo(order2);
        });

        var validationResult = _validationService.ValidateEntity(entityType);

        var entityModel = new EntityModel
        {
            EntityType = entityType,
            TopicAttribute = topicAttribute,
            KeyProperties = keyProperties,
            AllProperties = allProperties,
            ValidationResult = validationResult
        };

        _entityModels[entityType] = entityModel;
        return new EntityModelBuilder<T>(entityModel);
    }

    // ✅ 修正: スキーマ登録の責任を上位層に移譲
    public async Task BuildAsync()
    {
        if (_isBuilt) return;

        var validationResult = ValidateAllEntities();

        if (!validationResult.IsValid)
        {
            var errorMessage = "モデル構築に失敗しました:" + Environment.NewLine;
            errorMessage += string.Join(Environment.NewLine, validationResult.Errors);
            throw new InvalidOperationException(errorMessage);
        }

        // Core層では基本検証のみ実行
        // スキーマ登録は上位層（Infrastructure/Messaging層）の責任

        _isBuilt = true;
    }

    public void Build()
    {
        if (_isBuilt) return;

        var validationResult = ValidateAllEntities();

        if (!validationResult.IsValid)
        {
            var errorMessage = "モデル構築に失敗しました:" + Environment.NewLine;
            errorMessage += string.Join(Environment.NewLine, validationResult.Errors);
            throw new InvalidOperationException(errorMessage);
        }

        _isBuilt = true;
    }

    public Dictionary<Type, EntityModel> GetEntityModels()
    {
        return new Dictionary<Type, EntityModel>(_entityModels);
    }

    public EntityModel? GetEntityModel(Type entityType)
    {
        return _entityModels.TryGetValue(entityType, out var model) ? model : null;
    }

    public EntityModel? GetEntityModel<T>() where T : class
    {
        return GetEntityModel(typeof(T));
    }

    public ValidationResult ValidateAllEntities()
    {
        var overallResult = new ValidationResult { IsValid = true };

        foreach (var entityModel in _entityModels.Values)
        {
            if (entityModel.ValidationResult == null) continue;

            if (!entityModel.ValidationResult.IsValid)
            {
                overallResult.IsValid = false;
            }

            overallResult.Errors.AddRange(entityModel.ValidationResult.Errors);
            overallResult.Warnings.AddRange(entityModel.ValidationResult.Warnings);
            overallResult.AutoCompletedSettings.AddRange(entityModel.ValidationResult.AutoCompletedSettings);
        }

        return overallResult;
    }

    public string GetModelSummary()
    {
        if (_entityModels.Count == 0)
            return "登録済みエンティティ: なし";

        var summary = new List<string> { $"登録済みエンティティ: {_entityModels.Count}件" };

        foreach (var entityModel in _entityModels.Values)
        {
            var entityName = entityModel.EntityType.Name;
            var topicName = entityModel.TopicAttribute?.TopicName ?? $"{entityName} (自動生成)";
            var keyCount = entityModel.KeyProperties.Length;
            var propCount = entityModel.AllProperties.Length;
            var validStatus = entityModel.IsValid ? "✅" : "❌";

            summary.Add($"  {validStatus} {entityName} → Topic: {topicName}, Keys: {keyCount}, Props: {propCount}");
        }

        return string.Join(Environment.NewLine, summary);
    }

    public bool IsBuilt => _isBuilt;
}