using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Avro.Cache
{
    public class EntityCacheStatus
    {
        public Type EntityType { get; set; } = null!;
        public long KeySerializerHits { get; set; }
        public long KeySerializerMisses { get; set; }
        public long ValueSerializerHits { get; set; }
        public long ValueSerializerMisses { get; set; }
        public long KeyDeserializerHits { get; set; }
        public long KeyDeserializerMisses { get; set; }
        public long ValueDeserializerHits { get; set; }
        public long ValueDeserializerMisses { get; set; }

        public double KeySerializerHitRate => GetHitRate(KeySerializerHits, KeySerializerMisses);
        public double ValueSerializerHitRate => GetHitRate(ValueSerializerHits, ValueSerializerMisses);
        public double KeyDeserializerHitRate => GetHitRate(KeyDeserializerHits, KeyDeserializerMisses);
        public double ValueDeserializerHitRate => GetHitRate(ValueDeserializerHits, ValueDeserializerMisses);
        public double OverallHitRate => GetHitRate(AllHits, AllMisses);

        private long AllHits => KeySerializerHits + ValueSerializerHits + KeyDeserializerHits + ValueDeserializerHits;
        private long AllMisses => KeySerializerMisses + ValueSerializerMisses + KeyDeserializerMisses + ValueDeserializerMisses;

        private static double GetHitRate(long hits, long misses)
        {
            var total = hits + misses;
            return total > 0 ? (double)hits / total : 0.0;
        }
    }
}
