using System;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Abstractions
{
    /// <summary>
    /// KSQL構文ビルダーの共通インターフェース
    /// 設計理由：各ビルダークラスの統一、責務明確化
    /// </summary>
    public interface IKsqlBuilder
    {
        /// <summary>
        /// 式木からKSQL構文を構築
        /// </summary>
        /// <param name="expression">対象式木</param>
        /// <returns>KSQL構文文字列</returns>
        string Build(Expression expression);

        /// <summary>
        /// ビルダー種別識別
        /// </summary>
        KsqlBuilderType BuilderType { get; }
    }

    /// <summary>
    /// ビルダー種別列挙
    /// </summary>
    public enum KsqlBuilderType
    {
        Select,
        Where,
        GroupBy,
        Having,
        Join,
        Window,
        Projection
    }
}