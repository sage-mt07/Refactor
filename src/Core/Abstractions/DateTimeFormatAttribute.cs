using System;

namespace KsqlDsl.Core.Abstractions;
[AttributeUsage(AttributeTargets.Property)]
public class DateTimeFormatAttribute : Attribute
{
    public string Format { get; set; }
    public string? Region { get; set; }

}
