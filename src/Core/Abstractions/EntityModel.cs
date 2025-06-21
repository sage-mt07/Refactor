using KsqlDsl.Query.Abstractions;
using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace KsqlDsl.Core.Abstractions;

internal class EntityModel
{
    public Type EntityType { get; set; } = null!;

    public TopicAttribute? TopicAttribute { get; set; }

    public PropertyInfo[] KeyProperties { get; set; } = Array.Empty<PropertyInfo>();

    public PropertyInfo[] AllProperties { get; set; } = Array.Empty<PropertyInfo>();

    public ValidationResult? ValidationResult { get; set; }

    public bool IsValid => ValidationResult?.IsValid ?? false;
    public StreamTableType StreamTableType
    {
        get
        {
            // 1. 明示的な属性指定をチェック
            var streamAttribute = EntityType.GetCustomAttribute<KsqlStreamAttribute>();
            if (streamAttribute != null)
                return StreamTableType.Stream;

            var tableAttribute = EntityType.GetCustomAttribute<TableAttribute>();
            if (tableAttribute != null)
                return StreamTableType.Table;

            // 2. キープロパティの有無で判定
            if (HasKeys())
                return StreamTableType.Table;

            // 3. デフォルトはStream
            return StreamTableType.Stream;
        }
    }
    /// <summary>
    /// Stream/Table型の明示的設定
    /// </summary>
    /// <param name="streamTableType">設定する型</param>
    public void SetStreamTableType(StreamTableType streamTableType)
    {
        _explicitStreamTableType = streamTableType;
    }

    private StreamTableType? _explicitStreamTableType;

    /// <summary>
    /// 明示的に設定されたStream/Table型を取得
    /// </summary>
    public StreamTableType GetExplicitStreamTableType()
    {
        return _explicitStreamTableType ?? StreamTableType;
    }

    /// <summary>
    /// キープロパティの有無を確認
    /// 設計理由：Stream/Table判定に必要、CoreExtensions.HasKeys()と同等機能
    /// </summary>
    public bool HasKeys()
    {
        return KeyProperties != null && KeyProperties.Length > 0;
    }

    /// <summary>
    /// 複合キーかどうかを確認
    /// </summary>
    public bool IsCompositeKey()
    {
        return KeyProperties != null && KeyProperties.Length > 1;
    }
}