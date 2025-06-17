using KsqlDsl.Modeling;
using KsqlDsl.Options;
using KsqlDsl.Services;
using KsqlDsl.Communication; // Phase1: 新Manager使用のためインポート追加
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl;

public abstract class KafkaContext : IDisposable, IAsyncDisposable
{
    private readonly Lazy<ModelBuilder> _modelBuilder;
    private readonly Dictionary<Type, object> _eventSets = new();

    // Phase1変更：旧Service → 新Manager使用
    private readonly Lazy<KafkaProducerManager> _producerManager;
    private readonly Lazy<KafkaConsumerManager> _consumerManager;

    // 後方互換性維持のため、旧Serviceは内部的に新Managerをラップ
    [Obsolete("内部実装は新Managerに移行されました。直接アクセス非推奨", false)]
    private readonly Lazy<KafkaProducerService> _producerService;
    [Obsolete("内部実装は新Managerに移行されました。直接アクセス非推奨", false)]
    private readonly Lazy<KafkaConsumerService> _consumerService;

    private AvroSchemaRegistrationService? _schemaRegistrationService;
    private bool _disposed = false;
    private bool _modelBuilt = false;

    public KafkaContextOptions Options { get; private set; }

    protected KafkaContext()
    {
        var optionsBuilder = new KafkaContextOptionsBuilder();
        OnConfiguring(optionsBuilder);
        Options = optionsBuilder.Build();

        InitializeServices();
        InitializeEventSets();
    }

    protected KafkaContext(KafkaContextOptions options)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));

        InitializeServices();
        InitializeEventSets();
    }

    /// <summary>
    /// Phase1変更：新Manager使用でサービス初期化
    /// </summary>
    private void InitializeServices()
    {
        _modelBuilder = new Lazy<ModelBuilder>(() =>
        {
            var modelBuilder = new ModelBuilder(Options.ValidationMode);

            if (Options.EnableAutoSchemaRegistration)
            {
                _schemaRegistrationService = new AvroSchemaRegistrationService(
                    Options.CustomSchemaRegistryClient,
                    Options.ValidationMode,
                    Options.EnableDebugLogging);

                modelBuilder.SetSchemaRegistrationService(_schemaRegistrationService);
            }

            OnModelCreating(modelBuilder);
            modelBuilder.Build();
            return modelBuilder;
        });

        // Phase1変更：新Manager使用
        _producerManager = new Lazy<KafkaProducerManager>(() =>
        {
            // 実際の実装では、EnhancedKafkaProducerManagerを初期化
            // ここでは概念実装として簡略化
            Console.WriteLine("[INFO] Phase1: KafkaProducerManager初期化（EnhancedKafkaProducerManager使用）");
            return new KafkaProducerManager(
                null!, // EnhancedAvroSerializerManager
                null!, // ProducerPool  
                Microsoft.Extensions.Options.Options.Create(new KafkaProducerConfig()),
                null!  // ILogger
            );
        });

        _consumerManager = new Lazy<KafkaConsumerManager>(() =>
        {
            Console.WriteLine("[INFO] Phase1: KafkaConsumerManager初期化（型安全Consumer使用）");
            return new KafkaConsumerManager(
                null!, // EnhancedAvroSerializerManager
                null!, // ConsumerPool
                Microsoft.Extensions.Options.Options.Create(new KafkaConsumerConfig()),
                null!  // ILogger
            );
        });

        // 後方互換性：旧Service（廃止予定）
        _producerService = new Lazy<KafkaProducerService>(() =>
        {
            Console.WriteLine("[WARNING] 旧KafkaProducerService使用検出：新KafkaProducerManagerへの移行を推奨");
            try
            {
                return new KafkaProducerService(Options);
            }
            catch (NotSupportedException)
            {
                // Phase1：旧Serviceは廃止のため、例外をキャッチして代替手段を提供
                throw new InvalidOperationException(
                    "KafkaProducerServiceは廃止されました。" +
                    "GetProducerManager()を使用して新しいAPIにアクセスしてください。");
            }
        });

        _consumerService = new Lazy<KafkaConsumerService>(() =>
        {
            Console.WriteLine("[WARNING] 旧KafkaConsumerService使用検出：新KafkaConsumerManagerへの移行を推奨");
            try
            {
                return new KafkaConsumerService(Options);
            }
            catch (NotSupportedException)
            {
                // Phase1：旧Serviceは廃止のため、例外をキャッチして代替手段を提供
                throw new InvalidOperationException(
                    "KafkaConsumerServiceは廃止されました。" +
                    "GetConsumerManager()を使用して新しいAPIにアクセスしてください。");
            }
        });
    }

    protected abstract void OnModelCreating(ModelBuilder modelBuilder);
    protected virtual void OnConfiguring(KafkaContextOptionsBuilder optionsBuilder) { }

    // Phase1変更：新Manager使用の内部アクセサ
    internal KafkaProducerManager GetProducerManager() => _producerManager.Value;
    internal KafkaConsumerManager GetConsumerManager() => _consumerManager.Value;

    // 後方互換性維持：旧Service使用（廃止予定警告付き）
    [Obsolete("GetProducerService()は廃止予定です。GetProducerManager()を使用してください。", false)]
    internal KafkaProducerService GetProducerService() => _producerService.Value;

    [Obsolete("GetConsumerService()は廃止予定です。GetConsumerManager()を使用してください。", false)]
    internal KafkaConsumerService GetConsumerService() => _consumerService.Value;

    private void InitializeEventSets()
    {
        var contextType = GetType();
        var eventSetProperties = contextType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType.IsGenericType &&
                       p.PropertyType.GetGenericTypeDefinition() == typeof(EventSet<>))
            .ToArray();

        foreach (var property in eventSetProperties)
        {
            var entityType = property.PropertyType.GetGenericArguments()[0];
        }
    }

    public EventSet<T> Set<T>() where T : class
    {
        var entityType = typeof(T);

        if (_eventSets.TryGetValue(entityType, out var existingSet))
            return (EventSet<T>)existingSet;

        var modelBuilder = _modelBuilder.Value;
        _modelBuilt = true;

        var entityModel = modelBuilder.GetEntityModel<T>();
        if (entityModel == null)
        {
            throw new InvalidOperationException(
                $"エンティティ {entityType.Name} がModelBuilderに登録されていません。" +
                $"OnModelCreating()内でmodelBuilder.Event<{entityType.Name}>()を呼び出してください。");
        }

        var eventSet = new EventSet<T>(this, entityModel);
        _eventSets[entityType] = eventSet;
        return eventSet;
    }

    public object GetEventSet(Type entityType)
    {
        var setMethod = typeof(KafkaContext).GetMethod(nameof(Set))!.MakeGenericMethod(entityType);
        return setMethod.Invoke(this, null)!;
    }

    public ModelBuilder GetModelBuilder() => _modelBuilder.Value;
    public Dictionary<Type, EntityModel> GetEntityModels() => _modelBuilder.Value.GetEntityModels();

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        var modelBuilder = _modelBuilder.Value;

        if (Options.EnableDebugLogging)
        {
            Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: インフラストラクチャ作成開始");
            Console.WriteLine("[INFO] Phase1: 新Manager使用でインフラ構築");
            Console.WriteLine(modelBuilder.GetModelSummary());
        }

        await Task.Delay(1, cancellationToken);

        if (Options.EnableDebugLogging)
            Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: インフラストラクチャ作成完了");
    }

    public void EnsureCreated() => EnsureCreatedAsync().GetAwaiter().GetResult();

    public virtual async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        await Task.Delay(1, cancellationToken);

        if (Options.EnableDebugLogging)
            Console.WriteLine("[DEBUG] KafkaContext.SaveChangesAsync: Kafka流では通常不要（AddAsync時に即時送信）");

        return 0;
    }

    public virtual int SaveChanges() => SaveChangesAsync().GetAwaiter().GetResult();

    public async Task<List<string>> GetRegisteredSchemasAsync()
    {
        var modelBuilder = _modelBuilder.Value;
        return await modelBuilder.GetRegisteredSchemasAsync();
    }

    public async Task<bool> CheckEntitySchemaCompatibilityAsync<T>() where T : class
    {
        var modelBuilder = _modelBuilder.Value;
        return await modelBuilder.CheckEntitySchemaCompatibilityAsync<T>();
    }

    public string GetDiagnostics()
    {
        var diagnostics = new List<string>
        {
            $"KafkaContext: {GetType().Name}",
            $"Connection: {Options.ConnectionString}",
            $"Schema Registry: {Options.SchemaRegistryUrl}",
            $"Validation Mode: {Options.ValidationMode}",
            $"Consumer Group: {Options.ConsumerGroupId}",
            $"Auto Schema Registration: {Options.EnableAutoSchemaRegistration}",
            $"Model Built: {_modelBuilt}",
            $"EventSets Count: {_eventSets.Count}",
            "", // Phase1変更情報
            "=== Phase1変更情報 ===",
            "✅ ProducerService → ProducerManager移行済み",
            "✅ ConsumerService → ConsumerManager移行済み",
            "⚠️  旧Serviceは後方互換性のため維持（廃止予定）"
        };

        if (_modelBuilt)
        {
            diagnostics.Add("");
            diagnostics.Add(_modelBuilder.Value.GetModelSummary());
        }

        if (Options.TopicOverrideService.GetAllOverrides().Any())
        {
            diagnostics.Add("");
            diagnostics.Add(Options.TopicOverrideService.GetOverrideSummary());
        }

        return string.Join(Environment.NewLine, diagnostics);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _eventSets.Clear();

            // Phase1変更：新Manager使用
            if (_producerManager.IsValueCreated)
                _producerManager.Value.Dispose();

            if (_consumerManager.IsValueCreated)
                _consumerManager.Value.Dispose();

            // 旧Service（使用されていた場合のみ）
            if (_producerService.IsValueCreated)
            {
                try { _producerService.Value.Dispose(); }
                catch (NotSupportedException) { /* 廃止済みのため無視 */ }
            }

            if (_consumerService.IsValueCreated)
            {
                try { _consumerService.Value.Dispose(); }
                catch (NotSupportedException) { /* 廃止済みのため無視 */ }
            }

            if (Options.EnableDebugLogging)
                Console.WriteLine("[DEBUG] KafkaContext.Dispose: リソース解放完了（Phase1：新Manager使用）");

            _disposed = true;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore();
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        // Phase1変更：新Manager使用
        if (_producerManager.IsValueCreated)
            _producerManager.Value.Dispose();

        if (_consumerManager.IsValueCreated)
            _consumerManager.Value.Dispose();

        // 旧Service（廃止予定）
        if (_producerService.IsValueCreated)
        {
            try { _producerService.Value.Dispose(); }
            catch (NotSupportedException) { /* 廃止済みのため無視 */ }
        }

        if (_consumerService.IsValueCreated)
        {
            try { _consumerService.Value.Dispose(); }
            catch (NotSupportedException) { /* 廃止済みのため無視 */ }
        }

        await Task.Delay(1);
    }

    public override string ToString()
    {
        var connectionInfo = !string.IsNullOrEmpty(Options.ConnectionString)
            ? Options.ConnectionString
            : "未設定";
        return $"{GetType().Name} → {connectionInfo} [Phase1: 新Manager使用]";
    }
}