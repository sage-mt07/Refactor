using KsqlDsl.Core.Modeling;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions
{
    /// <summary>
    /// KafkaContextの抽象定義
    /// DbContext風の統一インターフェース
    /// </summary>
    public interface IKafkaContext : IDisposable, IAsyncDisposable
    {
        IEntitySet<T> Set<T>() where T : class;
        object GetEventSet(Type entityType);

        Task<int> SaveChangesAsync(CancellationToken cancellationToken = default);
        int SaveChanges();

        Task EnsureCreatedAsync(CancellationToken cancellationToken = default);
        void EnsureCreated();

        Dictionary<Type, EntityModel> GetEntityModels();
        string GetDiagnostics();
    }
}