using System;

namespace KsqlDsl.Serialization.Avro.Core;

/// <summary>
/// Avroスキーマ生成専門クラス
/// 設計原則: Serialization層の責務のみ（Query層への依存禁止）
/// </summary>
public static class SchemaGenerator
{
    public static string GenerateSchema<T>() => UnifiedSchemaGenerator.GenerateSchema<T>();
    public static string GenerateSchema(Type type) => UnifiedSchemaGenerator.GenerateSchema(type);
    public static string GenerateKeySchema<T>() => UnifiedSchemaGenerator.GenerateKeySchema<T>();
    public static string GenerateKeySchema(Type keyType) => UnifiedSchemaGenerator.GenerateKeySchema(keyType);
    public static bool ValidateSchema(string schema) => UnifiedSchemaGenerator.ValidateSchema(schema);
}