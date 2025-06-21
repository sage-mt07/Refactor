using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Producers.Core
{
    /// <summary>
    /// バッチコンテナ
    /// </summary>
    public class KafkaBatch<TValue, TKey>
       where TValue : class
       where TKey : notnull
    {
        public List<KafkaMessage<TValue, TKey>> Messages { get; set; } = new();

        public DateTime BatchStartTime { get; set; }
        public DateTime BatchEndTime { get; set; }
        public TimeSpan ProcessingTime => BatchEndTime - BatchStartTime;
        public int TotalMessages => Messages.Count;
        public bool IsEmpty => Messages.Count == 0;

        /// <summary>
        /// バッチ処理完了時のコミット
        /// </summary>
        public Task CommitAsync() => Task.CompletedTask; // 実装時に適切な処理を追加
    }


}
