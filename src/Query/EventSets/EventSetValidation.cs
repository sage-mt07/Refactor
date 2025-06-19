using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Attributes;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace KsqlDsl.Query.EventSets;

/// <summary>
/// EventSet型検証・Null確認
/// 設計理由：バリデーション責務の分離、Strictモード対応
/// </summary>
public partial class EventSetValidation<T> : EventSetStreaming<T> where T : class
{
    internal EventSetValidation(KafkaContext context, EntityModel entityModel)
        : base(context, entityModel) { }

    internal EventSetValidation(KafkaContext context, EntityModel entityModel, Expression expression)
        : base(context, entityModel, expression) { }

    /// <summary>
    /// クエリ実行前バリデーション強化版
    /// </summary>
    protected new virtual void ValidateQueryBeforeExecution()
    {
        // 基底クラスのバリデーション
        base.ValidateQueryBeforeExecution();

        // EntityModelバリデーション
        if (_entityModel == null)
        {
            throw new InvalidOperationException($"EntityModel is not configured for {typeof(T).Name}");
        }

        if (!_entityModel.IsValid)
        {
            var errors = _entityModel.ValidationResult?.Errors ?? new List<string> { "Unknown validation error" };
            throw new InvalidOperationException(
                $"EntityModel validation failed for {typeof(T).Name}: {string.Join("; ", errors)}");
        }

        // Expression バリデーション
        if (_expression == null)
        {
            throw new InvalidOperationException($"Query expression is null for {typeof(T).Name}");
        }

        // 未サポート操作チェック強化
        try
        {
            CheckForUnsupportedOperations(_expression);
        }
        catch (NotSupportedException ex)
        {
            throw new NotSupportedException(
                $"Unsupported LINQ operation detected in query for {typeof(T).Name}: {ex.Message}", ex);
        }

        // Strictモード時の追加チェック
        if (_context.Options.ValidationMode == ValidationMode.Strict)
        {
            ValidateStrictModeRequirements();
        }
    }

    /// <summary>
    /// クエリ結果後処理バリデーション強化版
    /// </summary>
    protected void ValidateQueryResults(List<T> results)
    {
        if (results == null)
        {
            throw new InvalidOperationException($"Query returned null results for {typeof(T).Name}");
        }

        // Strictモード時の追加バリデーション
        if (_context.Options.ValidationMode == ValidationMode.Strict)
        {
            foreach (var result in results)
            {
                if (result == null)
                {
                    throw new InvalidOperationException(
                        $"Query returned null entity in results for {typeof(T).Name}");
                }

                ValidateEntityStrict(result);
            }
        }
    }

    /// <summary>
    /// エンティティバリデーション拡張版
    /// </summary>
    protected override void ValidateEntity(T entity)
    {
        base.ValidateEntity(entity);

        // Strictモード時の追加バリデーション
        if (_context.Options.ValidationMode == ValidationMode.Strict)
        {
            ValidateEntityStrict(entity);
        }
    }

    /// <summary>
    /// Strictモードエンティティバリデーション
    /// </summary>
    private void ValidateEntityStrict(T entity)
    {
        var entityType = typeof(T);
        var properties = entityType.GetProperties();

        foreach (var property in properties)
        {
            var value = property.GetValue(entity);

            // MaxLength validation for string properties
            var maxLengthAttr = property.GetCustomAttribute<MaxLengthAttribute>();
            if (maxLengthAttr != null && value is string stringValue)
            {
                if (stringValue.Length > maxLengthAttr.Length)
                {
                    throw new InvalidOperationException(
                        $"Property '{property.Name}' exceeds maximum length of {maxLengthAttr.Length}. Current length: {stringValue.Length}");
                }
            }

            // Required property validation
            if (IsRequiredProperty(property) && value == null)
            {
                throw new InvalidOperationException(
                    $"Required property '{property.Name}' cannot be null for entity type '{entityType.Name}'");
            }

            // Type compatibility validation
            ValidatePropertyTypeCompatibility(property, value);
        }
    }

    /// <summary>
    /// 未サポート操作チェック
    /// </summary>
    private void CheckForUnsupportedOperations(Expression expression)
    {
        var visitor = new UnsupportedOperationVisitor();
        visitor.Visit(expression);
    }

    /// <summary>
    /// Strictモード要件バリデーション
    /// </summary>
    private void ValidateStrictModeRequirements()
    {
        // スキーマ整合性チェック
        ValidateSchemaConsistency();

        // パフォーマンス警告チェック
        ValidatePerformanceImpact();
    }

    /// <summary>
    /// スキーマ整合性チェック
    /// </summary>
    private void ValidateSchemaConsistency()
    {
        var entityType = typeof(T);
        var properties = entityType.GetProperties();

        // 全プロパティがシリアライズ可能かチェック
        foreach (var property in properties)
        {
            if (property.GetCustomAttribute<KafkaIgnoreAttribute>() != null)
                continue;

            if (!IsSerializableType(property.PropertyType))
            {
                throw new InvalidOperationException(
                    $"Property '{property.Name}' of type '{property.PropertyType.Name}' is not serializable in Avro schema for {entityType.Name}");
            }
        }
    }

    /// <summary>
    /// パフォーマンス影響バリデーション
    /// </summary>
    private void ValidatePerformanceImpact()
    {
        // 複雑なクエリの警告
        var complexityScore = CalculateQueryComplexity(_expression);
        if (complexityScore > 10)
        {
            if (_context.Options.EnableDebugLogging)
            {
                Console.WriteLine($"[WARNING] High query complexity detected (score: {complexityScore}) for {typeof(T).Name}");
            }
        }
    }

    /// <summary>
    /// プロパティが必須かどうかの判定
    /// </summary>
    private bool IsRequiredProperty(PropertyInfo property)
    {
        // キープロパティは必須
        if (property.GetCustomAttribute<KeyAttribute>() != null)
            return true;

        // Nullable型は非必須
        if (Nullable.GetUnderlyingType(property.PropertyType) != null)
            return false;

        // 参照型のnull許可状態をチェック
        if (!property.PropertyType.IsValueType)
        {
            try
            {
                var nullabilityContext = new NullabilityInfoContext();
                var nullabilityInfo = nullabilityContext.Create(property);
                return nullabilityInfo.WriteState == NullabilityState.NotNull;
            }
            catch
            {
                return false; // Nullable context取得失敗時は非必須扱い
            }
        }

        return true; // 値型は基本的に必須
    }

    /// <summary>
    /// プロパティ型互換性バリデーション
    /// </summary>
    private void ValidatePropertyTypeCompatibility(PropertyInfo property, object? value)
    {
        if (value == null) return;

        var expectedType = property.PropertyType;
        var actualType = value.GetType();

        if (!expectedType.IsAssignableFrom(actualType))
        {
            throw new InvalidOperationException(
                $"Property '{property.Name}' expected type '{expectedType.Name}' but got '{actualType.Name}'");
        }
    }

    /// <summary>
    /// シリアライズ可能型判定
    /// </summary>
    private bool IsSerializableType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        return underlyingType.IsPrimitive ||
               underlyingType == typeof(string) ||
               underlyingType == typeof(decimal) ||
               underlyingType == typeof(DateTime) ||
               underlyingType == typeof(DateTimeOffset) ||
               underlyingType == typeof(Guid) ||
               underlyingType == typeof(byte[]) ||
               underlyingType.IsEnum;
    }

    /// <summary>
    /// クエリ複雑度計算
    /// </summary>
    private int CalculateQueryComplexity(Expression expression)
    {
        var visitor = new ComplexityCalculatorVisitor();
        visitor.Visit(expression);
        return visitor.ComplexityScore;
    }

    /// <summary>
    /// 未サポート操作検出Visitor
    /// </summary>
    private class UnsupportedOperationVisitor : ExpressionVisitor
    {
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var methodName = node.Method.Name;

            switch (methodName)
            {
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                    throw new NotSupportedException($"ORDER BY operations are not supported in ksqlDB: {methodName}");

                case "Distinct":
                    throw new NotSupportedException("DISTINCT operations are not supported in ksqlDB");

                case "Union":
                case "Intersect":
                case "Except":
                    throw new NotSupportedException($"Set operations are not supported in ksqlDB: {methodName}");
            }

            return base.VisitMethodCall(node);
        }
    }

    /// <summary>
    /// クエリ複雑度計算Visitor
    /// </summary>
    private class ComplexityCalculatorVisitor : ExpressionVisitor
    {
        public int ComplexityScore { get; private set; } = 0;

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var methodName = node.Method.Name;

            // メソッド種別による複雑度加算
            switch (methodName)
            {
                case "Where":
                    ComplexityScore += 1;
                    break;
                case "Select":
                    ComplexityScore += 1;
                    break;
                case "GroupBy":
                    ComplexityScore += 3;
                    break;
                case "Join":
                    ComplexityScore += 5;
                    break;
                case "Sum":
                case "Count":
                case "Max":
                case "Min":
                case "Average":
                    ComplexityScore += 2;
                    break;
                default:
                    ComplexityScore += 1;
                    break;
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            ComplexityScore += 1;
            return base.VisitBinary(node);
        }
    }
}