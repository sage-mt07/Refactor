using KsqlDsl.Core.Modeling;
using KsqlDsl.Query.Abstractions;
using System;
using System.Linq.Expressions;

namespace KsqlDsl.Query.EventSets
{
    /// <summary>
    /// EventSet統合ファサード - 既存APIとの互換性維持
    /// 設計理由：分割後も既存コードが動作するよう互換性保持
    /// </summary>
    public class EventSet<T> : EventSetValidation<T> where T : class
    {
        internal EventSet(KafkaContext context, EntityModel entityModel)
            : base(context, entityModel) { }

        internal EventSet(KafkaContext context, EntityModel entityModel, Expression expression)
            : base(context, entityModel, expression) { }

        // ファクトリーメソッド - 将来的に他の実装に切り替え可能
        public static EventSet<T> Create(KafkaContext context, EntityModel entityModel)
        {
            return new EventSet<T>(context, entityModel);
        }

        public static EventSet<T> Create(KafkaContext context, EntityModel entityModel, Expression expression)
        {
            return new EventSet<T>(context, entityModel, expression);
        }


    }
}