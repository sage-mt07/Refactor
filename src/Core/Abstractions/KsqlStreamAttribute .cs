using System;

namespace KsqlDsl.Core.Abstractions
{
    [AttributeUsage(AttributeTargets.Class)]
    public class KsqlStreamAttribute : Attribute
    {
        public string? StreamName { get; set; }
        public int? Partitions { get; set; }
    }
}
