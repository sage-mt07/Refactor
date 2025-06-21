//using KsqlDsl.Core.Modeling;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Core.Abstractions;

/// <summary>
/// KafkaContextの抽象定義
/// DbContext風の統一インターフェース
/// </summary>
public interface IKafkaContext : IDisposable, IAsyncDisposable
{
    IEntitySet<T> Set<T>() where T : class;
    object GetEventSet(Type entityType);


    Dictionary<Type, EntityModel> GetEntityModels();

}