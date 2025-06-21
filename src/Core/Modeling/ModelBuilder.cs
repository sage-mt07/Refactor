using KsqlDsl.Configuration;
using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using KsqlDsl.Core.Extensions;
namespace KsqlDsl.Core.Modeling;
internal class ModelBuilder
{
    private readonly Dictionary<Type, EntityModel> _entityModels = new();
    private readonly ValidationMode _validationMode;

    public ModelBuilder(ValidationMode validationMode = ValidationMode.Strict)
    {
        _validationMode = validationMode;
    }

    public EntityModel? GetEntityModel<T>() where T : class
    {
        return GetEntityModel(typeof(T));
    }

    public EntityModel? GetEntityModel(Type entityType)
    {
        _entityModels.TryGetValue(entityType, out var model);
        return model;
    }

    public void AddEntityModel<T>() where T : class
    {
        AddEntityModel(typeof(T));
    }

    public void AddEntityModel(Type entityType)
    {
        if (_entityModels.ContainsKey(entityType))
            return;

        var entityModel = CreateEntityModelFromType(entityType);
        _entityModels[entityType] = entityModel;
    }

    public Dictionary<Type, EntityModel> GetAllEntityModels()
    {
        return new Dictionary<Type, EntityModel>(_entityModels);
    }

    public string GetModelSummary()
    {
        if (_entityModels.Count == 0)
            return "ModelBuilder: No entities configured";

        var summary = new List<string>
            {
                $"ModelBuilder: {_entityModels.Count} entities configured",
                $"Validation Mode: {_validationMode}",
                ""
            };

        foreach (var (entityType, model) in _entityModels.OrderBy(x => x.Key.Name))
        {
            var status = model.IsValid ? "✅" : "❌";
            summary.Add($"{status} {entityType.Name} → {model.GetTopicName()} ({model.StreamTableType}, Keys: {model.KeyProperties.Length})");

            if (model.ValidationResult != null && !model.ValidationResult.IsValid)
            {
                foreach (var error in model.ValidationResult.Errors)
                {
                    summary.Add($"   Error: {error}");
                }
            }

            if (model.ValidationResult != null && model.ValidationResult.Warnings.Count > 0)
            {
                foreach (var warning in model.ValidationResult.Warnings)
                {
                    summary.Add($"   Warning: {warning}");
                }
            }
        }

        return string.Join(Environment.NewLine, summary);
    }

    public bool ValidateAllModels()
    {
        bool allValid = true;

        foreach (var (entityType, model) in _entityModels)
        {
            var validation = ValidateEntityModel(entityType, model);
            model.ValidationResult = validation;

            if (!validation.IsValid)
            {
                allValid = false;
                if (_validationMode == ValidationMode.Strict)
                {
                    throw new InvalidOperationException($"Entity model validation failed for {entityType.Name}: {string.Join(", ", validation.Errors)}");
                }
            }
        }

        return allValid;
    }

    private EntityModel CreateEntityModelFromType(Type entityType)
    {
        var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
        var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

        // Key プロパティをOrder順にソート
        Array.Sort(keyProperties, (p1, p2) =>
        {
            var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
            var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
            return order1.CompareTo(order2);
        });

        var model = new EntityModel
        {
            EntityType = entityType,
            TopicAttribute = topicAttribute,
            AllProperties = allProperties,
            KeyProperties = keyProperties
        };

        // 検証実行
        model.ValidationResult = ValidateEntityModel(entityType, model);

        return model;
    }

    private ValidationResult ValidateEntityModel(Type entityType, EntityModel model)
    {
        var result = new ValidationResult { IsValid = true };

        // エンティティ型の基本検証
        if (!entityType.IsClass || entityType.IsAbstract)
        {
            result.IsValid = false;
            result.Errors.Add($"Entity type {entityType.Name} must be a concrete class");
        }

        // Topic属性の検証
        if (model.TopicAttribute == null)
        {
            if (_validationMode == ValidationMode.Strict)
            {
                result.IsValid = false;
                result.Errors.Add($"Entity {entityType.Name} must have [Topic] attribute");
            }
            else
            {
                result.Warnings.Add($"Entity {entityType.Name} does not have [Topic] attribute, using class name as topic");
            }
        }

        // プロパティの検証
        foreach (var property in model.AllProperties)
        {
            if (!IsValidPropertyType(property.PropertyType))
            {
                result.Warnings.Add($"Property {property.Name} has potentially unsupported type {property.PropertyType.Name}");
            }
        }

        // キープロパティの検証
        if (model.KeyProperties.Length == 0)
        {
            result.Warnings.Add($"Entity {entityType.Name} has no [Key] properties, will be treated as Stream");
        }

        return result;
    }

    private bool IsValidPropertyType(Type propertyType)
    {
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        return underlyingType.IsPrimitive ||
               underlyingType == typeof(string) ||
               underlyingType == typeof(decimal) ||
               underlyingType == typeof(DateTime) ||
               underlyingType == typeof(DateTimeOffset) ||
               underlyingType == typeof(Guid) ||
               underlyingType == typeof(byte[]);
    }
}