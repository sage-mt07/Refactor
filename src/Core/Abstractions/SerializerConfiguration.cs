using System;

namespace KsqlDsl.Core.Abstractions
{
    public class SerializerConfiguration<T> where T : class
    {
        public object KeySerializer { get; set; } = default!;
        public object ValueSerializer { get; set; } = default!;
        public int KeySchemaId { get; set; }
        public int ValueSchemaId { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
}
