using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Context;

public abstract class KafkaContextCore : IKafkaContext
{
    private readonly Dictionary<Type, EntityModel> _entityModels = new();
    private readonly Dictionary<Type, object> _entitySets = new();
    protected readonly KafkaContextOptions Options;
    private readonly ILogger<KafkaContextCore> _logger;
    private bool _disposed = false;

    protected KafkaContextCore()
    {
        Options = new KafkaContextOptions();
        _logger = NullLogger<KafkaContextCore>.Instance;
        InitializeEntityModels();
    }

    protected KafkaContextCore(KafkaContextOptions options)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = Options.LoggerFactory.CreateLoggerOrNull<KafkaContextCore>();
        InitializeEntityModels();
    }

    public IEntitySet<T> Set<T>() where T : class
    {
        var entityType = typeof(T);

        if (_entitySets.TryGetValue(entityType, out var existingSet))
        {
            return (IEntitySet<T>)existingSet;
        }

        var entityModel = GetOrCreateEntityModel<T>();
        var entitySet = CreateEntitySet<T>(entityModel);
        _entitySets[entityType] = entitySet;

        return entitySet;
    }

    public object GetEventSet(Type entityType)
    {
        if (_entitySets.TryGetValue(entityType, out var entitySet))
        {
            return entitySet;
        }

        var entityModel = GetOrCreateEntityModel(entityType);
        var setType = typeof(IEntitySet<>).MakeGenericType(entityType);
        var createdSet = CreateEntitySet(entityType, entityModel);
        _entitySets[entityType] = createdSet;

        return createdSet;
    }

    public virtual Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        // Core層では基本実装のみ提供
        return Task.FromResult(0);
    }

    public virtual int SaveChanges()
    {
        // Core層では基本実装のみ提供
        return 0;
    }

    public virtual Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        // Core層では基本実装のみ提供
        _logger?.LogDebug("KafkaContextCore.EnsureCreatedAsync completed");
        return Task.CompletedTask;
    }

    public virtual void EnsureCreated()
    {
        // Core層では基本実装のみ提供
        _logger?.LogDebug("KafkaContextCore.EnsureCreated completed");
    }

    public Dictionary<Type, EntityModel> GetEntityModels()
    {
        return new Dictionary<Type, EntityModel>(_entityModels);
    }

    public virtual string GetDiagnostics()
    {
        var diagnostics = new List<string>
            {
                "=== KafkaContextCore診断情報 ===",
                $"エンティティモデル数: {_entityModels.Count}",
                $"エンティティセット数: {_entitySets.Count}",
                ""
            };

        if (_entityModels.Count > 0)
        {
            diagnostics.Add("=== エンティティモデル ===");
            foreach (var (type, model) in _entityModels)
            {
                var status = model.IsValid ? "✅" : "❌";
                diagnostics.Add($"{status} {type.Name} → {model.GetTopicName()} (Keys: {model.KeyProperties.Length})");
            }
        }

        return string.Join(Environment.NewLine, diagnostics);
    }

    protected virtual IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) where T : class
    {
        // 具象クラスでオーバーライド
        throw new NotImplementedException("CreateEntitySet must be implemented by concrete class");
    }

    protected virtual object CreateEntitySet(Type entityType, EntityModel entityModel)
    {
        // リフレクションを使用してジェネリックメソッドを呼び出し
        var method = GetType().GetMethod(nameof(CreateEntitySet), 1, new[] { typeof(EntityModel) });
        var genericMethod = method!.MakeGenericMethod(entityType);
        return genericMethod.Invoke(this, new object[] { entityModel })!;
    }

    private void InitializeEntityModels()
    {
        // サブクラスでOnModelCreatingを呼び出すための準備
        _logger?.LogDebug("Entity models initialization started");
    }

    private EntityModel GetOrCreateEntityModel<T>() where T : class
    {
        return GetOrCreateEntityModel(typeof(T));
    }

    private EntityModel GetOrCreateEntityModel(Type entityType)
    {
        if (_entityModels.TryGetValue(entityType, out var existingModel))
        {
            return existingModel;
        }

        var entityModel = CreateEntityModelFromType(entityType);
        _entityModels[entityType] = entityModel;
        return entityModel;
    }

    private EntityModel CreateEntityModelFromType(Type entityType)
    {
        var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
        var allProperties = entityType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

        var model = new EntityModel
        {
            EntityType = entityType,
            TopicAttribute = topicAttribute,
            AllProperties = allProperties,
            KeyProperties = keyProperties
        };

        // 基本検証
        var validation = new ValidationResult { IsValid = true };

        if (topicAttribute == null)
        {
            validation.Warnings.Add($"No [Topic] attribute found for {entityType.Name}");
        }

        if (keyProperties.Length == 0)
        {
            validation.Warnings.Add($"No [Key] properties found for {entityType.Name}");
        }

        model.ValidationResult = validation;

        return model;
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            foreach (var entitySet in _entitySets.Values)
            {
                if (entitySet is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _entitySets.Clear();
            _entityModels.Clear();
            _disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public virtual async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore();
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        foreach (var entitySet in _entitySets.Values)
        {
            if (entitySet is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync();
            }
            else if (entitySet is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
        _entitySets.Clear();
        await Task.CompletedTask;
    }

    public override string ToString()
    {
        return $"KafkaContextCore: {_entityModels.Count} entities, {_entitySets.Count} sets";
    }
}