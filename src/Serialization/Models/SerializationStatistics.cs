using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Models
{
    public class SerializationStatistics
    {
        public double HitRate => TotalSerializations > 0 ? (double)CacheHits / TotalSerializations : 0.0;
        public TimeSpan AverageLatency { get; set; }
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
        public long TotalSerializations;     // ✅ プロパティ → フィールド
        public long TotalDeserializations;   // ✅ プロパティ → フィールド
        public long CacheHits;               // ✅ プロパティ → フィールド
        public long CacheMisses;             // ✅ プロパティ → フィールド

    }
}
