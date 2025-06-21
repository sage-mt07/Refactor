using System.Linq.Expressions;

namespace KsqlDsl.Query.Abstractions
{
    /// <summary>
    /// LINQ式からKSQL文への変換責務を定義
    /// 設計理由：変換ロジックの抽象化、テスタビリティ向上
    /// </summary>
    public interface IQueryTranslator
    {
        /// <summary>
        /// LINQ式をKSQL文に変換
        /// </summary>
        /// <param name="expression">LINQ式木</param>
        /// <param name="topicName">対象トピック名</param>
        /// <param name="isPullQuery">Pull Query判定</param>
        /// <returns>KSQL文字列</returns>
        string ToKsql(Expression expression, string topicName, bool isPullQuery = false);

        /// <summary>
        /// 変換診断情報取得
        /// </summary>
        string GetDiagnostics();

        /// <summary>
        /// Pull Query判定
        /// </summary>
        bool IsPullQuery();
    }
}