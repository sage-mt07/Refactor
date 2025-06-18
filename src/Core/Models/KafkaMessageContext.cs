using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Models
{
    /// <summary>
    /// メッセージ送信時のコンテキスト情報
    /// 設計理由：横断的関心事（トレーシング、パーティション指定等）の管理
    /// </summary>
    public class KafkaMessageContext
    {
        public string? MessageId { get; set; }
        public string? CorrelationId { get; set; }
        public int? TargetPartition { get; set; }
        public Dictionary<string, object> Headers { get; set; } = new();
        public TimeSpan? Timeout { get; set; }
        public Dictionary<string, object> Tags { get; set; } = new();

        // OpenTelemetry連携
        public System.Diagnostics.ActivityContext? ActivityContext { get; set; }
    }


}
