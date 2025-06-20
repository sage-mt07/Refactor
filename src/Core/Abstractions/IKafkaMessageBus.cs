using KsqlDsl.Configuration.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Abstractions;

public interface IKafkaMessageBus : IDisposable
{
    Task SendAsync<T>(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default) where T : class;

    Task SendBatchAsync<T>(IEnumerable<T> messages, KafkaMessageContext? context = null, CancellationToken cancellationToken = default) where T : class;

    IAsyncEnumerable<T> ConsumeAsync<T>(KafkaSubscriptionOptions? options = null, CancellationToken cancellationToken = default) where T : class;

    Task<List<T>> FetchAsync<T>(KafkaFetchOptions options, CancellationToken cancellationToken = default) where T : class;

    Task<CoreHealthReport> GetHealthReportAsync();
    CoreDiagnostics GetDiagnostics();
}
