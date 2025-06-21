using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Query.Pipeline
{
    public class DerivedObjectInfo
    {
        public string Name { get; set; } = string.Empty;
        public DerivedObjectType Type { get; set; }
        public string BaseObject { get; set; } = string.Empty;
        public Expression Expression { get; set; } = Expression.Empty();
        public DateTime CreatedAt { get; set; }
    }
}
