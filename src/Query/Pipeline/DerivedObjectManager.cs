using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Threading.Tasks;
using KsqlDsl.Core.Extensions;
namespace KsqlDsl.Query.Pipeline;

internal class DerivedObjectManager : IDerivedObjectManager
{
    private readonly KsqlDbExecutor _executor;
    private readonly DDLQueryGenerator _ddlGenerator;
    private readonly StreamTableAnalyzer _analyzer;
    private readonly ConcurrentDictionary<string, DerivedObjectInfo> _derivedObjects;
    private readonly ILogger _logger;

    public DerivedObjectManager(
        KsqlDbExecutor executor,
        DDLQueryGenerator ddlGenerator,
        StreamTableAnalyzer analyzer,
        ILoggerFactory? loggerFactory)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _ddlGenerator = ddlGenerator ?? throw new ArgumentNullException(nameof(ddlGenerator));
        _analyzer = analyzer ?? throw new ArgumentNullException(nameof(analyzer));
        _derivedObjects = new ConcurrentDictionary<string, DerivedObjectInfo>();
        _logger = loggerFactory.CreateLoggerOrNull<DerivedObjectManager>();
    }

    public string CreateDerivedStream(string baseName, Expression linqExpression)
    {
        var derivedName = GenerateDerivedName(baseName, "STREAM");
        var createQuery = _ddlGenerator.GenerateCreateStreamAs(derivedName, baseName, linqExpression);

        _executor.ExecuteDDL(createQuery);

        var derivedInfo = new DerivedObjectInfo
        {
            Name = derivedName,
            Type = DerivedObjectType.Stream,
            BaseObject = baseName,
            Expression = linqExpression,
            CreatedAt = DateTime.UtcNow
        };

        _derivedObjects.TryAdd(derivedName, derivedInfo);

        _logger.LogDebug("Created derived stream: {DerivedName} from {BaseName}", derivedName, baseName);

        return derivedName;
    }

    public string CreateDerivedTable(string baseName, Expression linqExpression)
    {
        var derivedName = GenerateDerivedName(baseName, "TABLE");
        var createQuery = _ddlGenerator.GenerateCreateTableAs(derivedName, baseName, linqExpression);

        _executor.ExecuteDDL(createQuery);

        var derivedInfo = new DerivedObjectInfo
        {
            Name = derivedName,
            Type = DerivedObjectType.Table,
            BaseObject = baseName,
            Expression = linqExpression,
            CreatedAt = DateTime.UtcNow
        };

        _derivedObjects.TryAdd(derivedName, derivedInfo);

        _logger.LogDebug("Created derived table: {DerivedName} from {BaseName}", derivedName, baseName);

        return derivedName;
    }

    public async Task<string> CreateDerivedStreamAsync(string baseName, Expression linqExpression)
    {
        var derivedName = GenerateDerivedName(baseName, "STREAM");
        var createQuery = _ddlGenerator.GenerateCreateStreamAs(derivedName, baseName, linqExpression);

        await _executor.ExecuteDDLAsync(createQuery);

        var derivedInfo = new DerivedObjectInfo
        {
            Name = derivedName,
            Type = DerivedObjectType.Stream,
            BaseObject = baseName,
            Expression = linqExpression,
            CreatedAt = DateTime.UtcNow
        };

        _derivedObjects.TryAdd(derivedName, derivedInfo);

        _logger.LogDebug("Created derived stream async: {DerivedName} from {BaseName}", derivedName, baseName);

        return derivedName;
    }

    public async Task<string> CreateDerivedTableAsync(string baseName, Expression linqExpression)
    {
        var derivedName = GenerateDerivedName(baseName, "TABLE");
        var createQuery = _ddlGenerator.GenerateCreateTableAs(derivedName, baseName, linqExpression);

        await _executor.ExecuteDDLAsync(createQuery);

        var derivedInfo = new DerivedObjectInfo
        {
            Name = derivedName,
            Type = DerivedObjectType.Table,
            BaseObject = baseName,
            Expression = linqExpression,
            CreatedAt = DateTime.UtcNow
        };

        _derivedObjects.TryAdd(derivedName, derivedInfo);

        _logger.LogDebug("Created derived table async: {DerivedName} from {BaseName}", derivedName, baseName);

        return derivedName;
    }

    public void CleanupDerivedObjects()
    {
        foreach (var derivedObject in _derivedObjects.Values)
        {
            try
            {
                var dropQuery = $"DROP {derivedObject.Type.ToString().ToUpper()} IF EXISTS {derivedObject.Name}";
                _executor.ExecuteDDL(dropQuery);
                _logger.LogDebug("Dropped derived object: {Name}", derivedObject.Name);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to drop derived object: {Name}", derivedObject.Name);
            }
        }

        _derivedObjects.Clear();
        _logger.LogInformation("Cleanup completed for {ObjectCount} derived objects", _derivedObjects.Count);
    }

    public async Task CleanupDerivedObjectsAsync()
    {
        foreach (var derivedObject in _derivedObjects.Values)
        {
            try
            {
                var dropQuery = $"DROP {derivedObject.Type.ToString().ToUpper()} IF EXISTS {derivedObject.Name}";
                await _executor.ExecuteDDLAsync(dropQuery);
                _logger.LogDebug("Dropped derived object async: {Name}", derivedObject.Name);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to drop derived object async: {Name}", derivedObject.Name);
            }
        }

        _derivedObjects.Clear();
        _logger.LogInformation("Async cleanup completed for derived objects");
    }

    private string GenerateDerivedName(string baseName, string objectType)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var hash = Math.Abs(baseName.GetHashCode()) % 10000;
        return $"{baseName.ToLower()}_{objectType.ToLower()}_{hash}_{timestamp}";
    }
}
