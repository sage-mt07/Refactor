using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Abstractions
{
    public class KafkaFetchOptions
    {
        /// <summary>
        /// 最大レコード数
        /// </summary>
        public int MaxRecords { get; set; } = 100;

        /// <summary>
        /// タイムアウト時間
        /// </summary>
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// コンシューマーグループID
        /// </summary>
        public string? GroupId { get; set; }
    }
}
