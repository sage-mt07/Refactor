using KsqlDsl.Core.Abstractions;
using System.Linq.Expressions;

namespace KsqlDsl.Query.Pipeline;

internal interface IDDLQueryGenerator
{
    string GenerateCreateStream(string streamName, string topicName, EntityModel entityModel);
    string GenerateCreateTable(string tableName, string topicName, EntityModel entityModel);
    string GenerateCreateStreamAs(string streamName, string baseObject, Expression linqExpression);
    string GenerateCreateTableAs(string tableName, string baseObject, Expression linqExpression);
}