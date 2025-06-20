using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace KsqlDsl.Query.EventSets;

internal class EventQueryProvider<T> : IQueryProvider where T : class
{
    private readonly IKafkaContext _context;
    private readonly EntityModel _entityModel;

    public EventQueryProvider(IKafkaContext context, EntityModel entityModel)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
    }

    public IQueryable CreateQuery(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        var elementType = expression.Type.GetGenericArguments().FirstOrDefault() ?? typeof(object);

        // elementTypeがclass制約を満たすかチェック
        if (!elementType.IsClass)
        {
            throw new ArgumentException($"Element type {elementType.Name} must be a reference type for EventSet<T>");
        }

        var queryableType = typeof(EventSet<>).MakeGenericType(elementType);

        // IKafkaContextをKafkaContextにキャスト
        var kafkaContext = _context as KafkaContext ??
            throw new InvalidOperationException("Context must be of type KafkaContext");

        return (IQueryable)Activator.CreateInstance(queryableType, kafkaContext, _entityModel, expression)!;
    }

    // 明示的なインターフェイス実装 - 型制約の問題を回避
    IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        // TElementがclass制約を満たすかチェック
        if (!typeof(TElement).IsClass)
        {
            throw new ArgumentException($"Element type {typeof(TElement).Name} must be a reference type for EventSet<T>");
        }

        // IKafkaContextをKafkaContextにキャスト
        var kafkaContext = _context as KafkaContext ??
            throw new InvalidOperationException("Context must be of type KafkaContext");

        // リフレクションを使用してEventSet<TElement>を作成
        var eventSetType = typeof(EventSet<>).MakeGenericType(typeof(TElement));
        var createMethod = eventSetType.GetMethod("Create", new[] { typeof(KafkaContext), typeof(EntityModel), typeof(Expression) });

        if (createMethod == null)
        {
            // Createメソッドが見つからない場合はコンストラクタを使用
            return (IQueryable<TElement>)Activator.CreateInstance(eventSetType, kafkaContext, _entityModel, expression)!;
        }

        return (IQueryable<TElement>)createMethod.Invoke(null, new object[] { kafkaContext, _entityModel, expression })!;
    }

    public object? Execute(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        // Core層統合：クエリ実行時の処理（ToList等）
        return new List<T>();
    }

    public TResult Execute<TResult>(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        var result = Execute(expression);
        return (TResult)result!;
    }
}