using KsqlDsl.Configuration.Validation;
using System;
using System.Reflection;

namespace KsqlDsl.Core.Abstractions;

public class EntityModel
{
    public Type EntityType { get; set; } = null!;

    public TopicAttribute? TopicAttribute { get; set; }

    public PropertyInfo[] KeyProperties { get; set; } = Array.Empty<PropertyInfo>();

    public PropertyInfo[] AllProperties { get; set; } = Array.Empty<PropertyInfo>();

    public ValidationResult? ValidationResult { get; set; }

    public bool IsValid => ValidationResult?.IsValid ?? false;
}