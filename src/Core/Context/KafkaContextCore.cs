using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Modeling;
using KsqlDsl.Modeling;
using KsqlDsl.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Context
{
    /// <summary>
    /// Core層KafkaContext実装
    /// 外部依存を除外したクリーンな実装
    /// </summary>
    public abstract class KafkaContextCore : IKafkaContext
    {
        private readonly Lazy<ModelBuilder> _modelBuilder;
        private readonly Dictionary<Type, object> _entitySets = new();
        private bool _disposed = false;
        private bool _modelBuilt = false;

        public KafkaContextOptions Options { get; private set; }

        protected KafkaContextCore()
        {
            var optionsBuilder = new KafkaContextOptionsBuilder();
            OnConfiguring(optionsBuilder);
            Options = optionsBuilder.Build();

            _modelBuilder = new Lazy<ModelBuilder>(CreateModelBuilder);
            InitializeEntitySets();
        }

        protected KafkaContextCore(KafkaContextOptions options)
        {
            Options = options ?? throw new ArgumentNullException(nameof(options));
            _modelBuilder = new Lazy<ModelBuilder>(CreateModelBuilder);
            InitializeEntitySets();
        }

        protected abstract void OnModelCreating(ModelBuilder modelBuilder);
        protected virtual void OnConfiguring(KafkaContextOptionsBuilder optionsBuilder) { }

        public IEntitySet<T> Set<T>() where T : class
        {
            var entityType = typeof(T);

            if (_entitySets.TryGetValue(entityType, out var existingSet))
                return (IEntitySet<T>)existingSet;

            var modelBuilder = _modelBuilder.Value;
            _modelBuilt = true;

            var entityModel = modelBuilder.GetEntityModel<T>();
            if (entityModel == null)
            {
                throw new InvalidOperationException(
                    $"エンティティ {entityType.Name} がModelBuilderに登録されていません。" +
                    $"OnModelCreating()内でmodelBuilder.Event<{entityType.Name}>()を呼び出してください。");
            }

            var entitySet = CreateEntitySet<T>(entityModel);
            _entitySets[entityType] = entitySet;
            return entitySet;
        }

        public object GetEventSet(Type entityType)
        {
            var setMethod = typeof(KafkaContextCore).GetMethod(nameof(Set))!.MakeGenericMethod(entityType);
            return setMethod.Invoke(this, null)!;
        }

        public virtual async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            await Task.Delay(1, cancellationToken);

            if (Options.EnableDebugLogging)
                Console.WriteLine("[DEBUG] KafkaContext.SaveChangesAsync: Kafka流では通常不要（AddAsync時に即時送信）");

            return 0;
        }

        public virtual int SaveChanges() => SaveChangesAsync().GetAwaiter().GetResult();

        public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
        {
            var modelBuilder = _modelBuilder.Value;

            if (Options.EnableDebugLogging)
            {
                Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: インフラストラクチャ作成開始");
                Console.WriteLine(modelBuilder.GetModelSummary());
            }

            await Task.Delay(1, cancellationToken);

            if (Options.EnableDebugLogging)
                Console.WriteLine("[DEBUG] KafkaContext.EnsureCreatedAsync: インフラストラクチャ作成完了");
        }

        public void EnsureCreated() => EnsureCreatedAsync().GetAwaiter().GetResult();

        public Dictionary<Type, EntityModel> GetEntityModels() => _modelBuilder.Value.GetEntityModels();

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
                $"EntitySets Count: {_entitySets.Count}"
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

        // 抽象メソッド：上位層での実装
        protected abstract IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) where T : class;

        private ModelBuilder CreateModelBuilder()
        {
            var modelBuilder = new ModelBuilder(Options.ValidationMode);
            OnModelCreating(modelBuilder);
            modelBuilder.Build();
            return modelBuilder;
        }

        private void InitializeEntitySets()
        {
            var contextType = GetType();
            var eventSetProperties = contextType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.PropertyType.IsGenericType &&
                           p.PropertyType.GetGenericTypeDefinition() == typeof(IEntitySet<>))
                .ToArray();

            foreach (var property in eventSetProperties)
            {
                var entityType = property.PropertyType.GetGenericArguments()[0];
            }
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
                _entitySets.Clear();

                if (Options.EnableDebugLogging)
                    Console.WriteLine("[DEBUG] KafkaContext.Dispose: リソース解放完了");

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
            await Task.Delay(1);
        }

        public override string ToString()
        {
            var connectionInfo = !string.IsNullOrEmpty(Options.ConnectionString)
                ? Options.ConnectionString
                : "未設定";
            return $"{GetType().Name} → {connectionInfo}";
        }
    }
}