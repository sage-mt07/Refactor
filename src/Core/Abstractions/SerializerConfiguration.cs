using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
