using KsqlDsl.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Messaging.Consumers.Core
{
    public class ConsumerInstance
    {
        public ConsumerKey ConsumerKey { get; set; } = default!;
        public PooledConsumer PooledConsumer { get; set; } = default!;
        public DateTime RentedAt { get; set; }
        public bool IsActive { get; set; }
    }
}
