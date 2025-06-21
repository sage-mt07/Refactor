using System.Linq.Expressions;

namespace KsqlDsl.Query.Pipeline;

public interface IDMLQueryGenerator
{
    string GenerateSelectAll(string objectName, bool isPullQuery = true);
    string GenerateSelectWithCondition(string objectName, Expression whereExpression, bool isPullQuery = true);
    string GenerateCountQuery(string objectName);
    string GenerateAggregateQuery(string objectName, Expression aggregateExpression);
}
