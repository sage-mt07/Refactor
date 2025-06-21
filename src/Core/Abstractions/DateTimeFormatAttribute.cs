using System;

namespace KsqlDsl.Core.Abstractions;
[AttributeUsage(AttributeTargets.Property)]
public class DateTimeFormatAttribute : Attribute
{
    public DateTimeFormatAttribute(string format = "yyyy-MM-dd HH:mm:ss")
    {
        Format = format ?? throw new ArgumentNullException(nameof(format));
    }
    public required string Format { get; set; }
    public string? Region { get; set; }

}
