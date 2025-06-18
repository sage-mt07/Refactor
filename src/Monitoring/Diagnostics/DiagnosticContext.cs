using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KsqlDsl.Monitoring.Abstractions;
using KsqlDsl.Avro;
using KsqlDsl.Serialization.Avro.Cache;

namespace KsqlDsl.Monitoring.Diagnostics
{
    /// <summary>
    /// 診断情報の統合コンテキスト
    /// 設計理由：各コンポーネントの診断情報を統合管理、トラブルシューティング支援
    /// </summary>
    public class DiagnosticContext : IDiagnosticsProvider
    {
        private readonly PerformanceMonitoringAvroCache _avroCache;
        private readonly List<IDiagnosticsProvider> _providers = new();

        public string ComponentName => "Integrated Diagnostics";
        public DiagnosticsCategory Category => DiagnosticsCategory.General;
        public DiagnosticsPriority Priority => DiagnosticsPriority.High;

        public DiagnosticContext(PerformanceMonitoringAvroCache avroCache)
        {
            _avroCache = avroCache ?? throw new ArgumentNullException(nameof(avroCache));
        }

        /// <summary>
        /// 診断プロバイダーの登録
        /// </summary>
        public void RegisterProvider(IDiagnosticsProvider provider)
        {
            if (provider == null)
                throw new ArgumentNullException(nameof(provider));

            _providers.Add(provider);
        }

        public async Task<DiagnosticsInfo> GetDiagnosticsAsync(CancellationToken cancellationToken = default)
        {
            var stopwatch = Stopwatch.StartNew();
            var diagnostics = new DiagnosticsInfo
            {
                ComponentName = ComponentName,
                Category = Category,
                Priority = Priority,
                CollectedAt = DateTime.UtcNow
            };

            try
            {
                // システム基本情報
                await CollectSystemInfoAsync(diagnostics, cancellationToken);

                // Avroキャッシュ診断情報
                await CollectAvroCacheDiagnosticsAsync(diagnostics, cancellationToken);

                // 登録されたプロバイダーからの診断情報収集
                await CollectProviderDiagnosticsAsync(diagnostics, cancellationToken);

                // パフォーマンス情報
                await CollectPerformanceDiagnosticsAsync(diagnostics, cancellationToken);

                stopwatch.Stop();
                diagnostics.CollectionDuration = stopwatch.Elapsed;

                return diagnostics;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                diagnostics.CollectionDuration = stopwatch.Elapsed;
                diagnostics.Errors.Add(new DiagnosticsError
                {
                    Message = "Failed to collect diagnostics",
                    Exception = ex,
                    Code = "DIAG_COLLECTION_FAILED"
                });

                return diagnostics;
            }
        }

        private async Task CollectSystemInfoAsync(DiagnosticsInfo diagnostics, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken);

            try
            {
                var process = Process.GetCurrentProcess();

                diagnostics.Entries.AddRange(new[]
                {
                    new DiagnosticsEntry
                    {
                        Key = "MachineName",
                        Value = Environment.MachineName,
                        Type = DiagnosticsEntryType.Info,
                        Description = "Host machine name"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "ProcessId",
                        Value = Environment.ProcessId,
                        Type = DiagnosticsEntryType.Info,
                        Description = "Current process ID"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "ThreadCount",
                        Value = process.Threads.Count,
                        Type = DiagnosticsEntryType.Gauge,
                        Description = "Number of active threads"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "WorkingSet",
                        Value = process.WorkingSet64,
                        Type = DiagnosticsEntryType.Gauge,
                        Description = "Process working set (bytes)"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "GCMemory",
                        Value = GC.GetTotalMemory(false),
                        Type = DiagnosticsEntryType.Gauge,
                        Description = "GC managed memory (bytes)"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "UpTime",
                        Value = DateTime.UtcNow - Process.GetCurrentProcess().StartTime.ToUniversalTime(),
                        Type = DiagnosticsEntryType.Info,
                        Description = "Process up time"
                    }
                });
            }
            catch (Exception ex)
            {
                diagnostics.Errors.Add(new DiagnosticsError
                {
                    Message = "Failed to collect system information",
                    Exception = ex,
                    Code = "SYSTEM_INFO_FAILED"
                });
            }
        }

        private async Task CollectAvroCacheDiagnosticsAsync(DiagnosticsInfo diagnostics, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken);

            try
            {
                var stats = _avroCache.GetExtendedStatistics();
                var baseStats = stats.BaseStatistics;

                diagnostics.Entries.AddRange(new[]
                {
                    new DiagnosticsEntry
                    {
                        Key = "Avro.CacheHitRate",
                        Value = baseStats.HitRate,
                        Type = DiagnosticsEntryType.Gauge,
                        Description = "Avro cache hit rate"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "Avro.CachedItems",
                        Value = baseStats.CachedItemCount,
                        Type = DiagnosticsEntryType.Gauge,
                        Description = "Number of cached items"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "Avro.TotalRequests",
                        Value = baseStats.TotalRequests,
                        Type = DiagnosticsEntryType.Counter,
                        Description = "Total cache requests"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "Avro.SlowOperationRate",
                        Value = stats.SlowOperationRate,
                        Type = DiagnosticsEntryType.Gauge,
                        Description = "Rate of slow operations"
                    },
                    new DiagnosticsEntry
                    {
                        Key = "Avro.EntityCount",
                        Value = stats.EntityStatistics.Count,
                        Type = DiagnosticsEntryType.Gauge,
                        Description = "Number of entities in cache"
                    }
                });

                // 健全性チェック
                if (baseStats.HitRate < 0.5)
                {
                    diagnostics.Warnings.Add(new DiagnosticsWarning
                    {
                        Message = $"Low Avro cache hit rate: {baseStats.HitRate:P2}",
                        Code = "AVRO_LOW_HIT_RATE"
                    });
                }

                if (stats.SlowOperationRate > 0.1)
                {
                    diagnostics.Warnings.Add(new DiagnosticsWarning
                    {
                        Message = $"High slow operation rate: {stats.SlowOperationRate:P2}",
                        Code = "AVRO_HIGH_SLOW_RATE"
                    });
                }
            }
            catch (Exception ex)
            {
                diagnostics.Errors.Add(new DiagnosticsError
                {
                    Message = "Failed to collect Avro cache diagnostics",
                    Exception = ex,
                    Code = "AVRO_DIAG_FAILED"
                });
            }
        }

        private async Task CollectProviderDiagnosticsAsync(DiagnosticsInfo diagnostics, CancellationToken cancellationToken)
        {
            var tasks = _providers.Select(async provider =>
            {
                try
                {
                    var providerDiag = await provider.GetDiagnosticsAsync(cancellationToken);

                    // プロバイダー情報をメイン診断に統合
                    foreach (var entry in providerDiag.Entries)
                    {
                        entry.Key = $"{provider.ComponentName}.{entry.Key}";
                        diagnostics.Entries.Add(entry);
                    }

                    diagnostics.Warnings.AddRange(providerDiag.Warnings);
                    diagnostics.Errors.AddRange(providerDiag.Errors);
                }
                catch (Exception ex)
                {
                    diagnostics.Errors.Add(new DiagnosticsError
                    {
                        Message = $"Failed to collect diagnostics from provider: {provider.ComponentName}",
                        Exception = ex,
                        Code = "PROVIDER_DIAG_FAILED",
                        Context = provider.ComponentName
                    });
                }
            });

            await Task.WhenAll(tasks);
        }

        private async Task CollectPerformanceDiagnosticsAsync(DiagnosticsInfo diagnostics, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken);

            try
            {
                var stats = _avroCache.GetExtendedStatistics();

                // パフォーマンス上位/下位エンティティの特定
                var performanceMetrics = stats.PerformanceMetrics.Values.ToList();

                if (performanceMetrics.Any())
                {
                    var slowestEntity = performanceMetrics.OrderByDescending(m => m.AverageDuration).First();
                    var fastestEntity = performanceMetrics.OrderBy(m => m.AverageDuration).First();

                    diagnostics.Entries.AddRange(new[]
                    {
                        new DiagnosticsEntry
                        {
                            Key = "Performance.SlowestEntityAvgMs",
                            Value = slowestEntity.AverageDuration.TotalMilliseconds,
                            Type = DiagnosticsEntryType.Gauge,
                            Description = "Slowest entity average duration (ms)"
                        },
                        new DiagnosticsEntry
                        {
                            Key = "Performance.FastestEntityAvgMs",
                            Value = fastestEntity.AverageDuration.TotalMilliseconds,
                            Type = DiagnosticsEntryType.Gauge,
                            Description = "Fastest entity average duration (ms)"
                        },
                        new DiagnosticsEntry
                        {
                            Key = "Performance.EntityCount",
                            Value = performanceMetrics.Count,
                            Type = DiagnosticsEntryType.Gauge,
                            Description = "Number of entities with performance data"
                        }
                    });

                    // パフォーマンス警告
                    var slowEntities = performanceMetrics.Where(m => m.AverageDuration.TotalMilliseconds > 100).ToList();
                    if (slowEntities.Any())
                    {
                        diagnostics.Warnings.Add(new DiagnosticsWarning
                        {
                            Message = $"{slowEntities.Count} entities have slow average duration (>100ms)",
                            Code = "PERF_SLOW_ENTITIES",
                            Context = slowEntities.Count
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                diagnostics.Errors.Add(new DiagnosticsError
                {
                    Message = "Failed to collect performance diagnostics",
                    Exception = ex,
                    Code = "PERF_DIAG_FAILED"
                });
            }
        }

        /// <summary>
        /// 診断情報のサマリ取得
        /// </summary>
        public async Task<string> GetDiagnosticsSummaryAsync(CancellationToken cancellationToken = default)
        {
            var diagnostics = await GetDiagnosticsAsync(cancellationToken);

            var summary = new List<string>
            {
                $"Diagnostics Summary - {diagnostics.ComponentName}",
                $"Collection Time: {diagnostics.CollectedAt:yyyy-MM-dd HH:mm:ss} (Duration: {diagnostics.CollectionDuration.TotalMilliseconds:F0}ms)",
                $"Entries: {diagnostics.Entries.Count}, Warnings: {diagnostics.Warnings.Count}, Errors: {diagnostics.Errors.Count}",
                ""
            };

            if (diagnostics.Errors.Any())
            {
                summary.Add("ERRORS:");
                summary.AddRange(diagnostics.Errors.Select(e => $"  - {e.Code}: {e.Message}"));
                summary.Add("");
            }

            if (diagnostics.Warnings.Any())
            {
                summary.Add("WARNINGS:");
                summary.AddRange(diagnostics.Warnings.Select(w => $"  - {w.Code}: {w.Message}"));
                summary.Add("");
            }

            summary.Add("KEY METRICS:");
            var keyMetrics = diagnostics.Entries
                .Where(e => e.Type == DiagnosticsEntryType.Gauge || e.Type == DiagnosticsEntryType.Counter)
                .OrderBy(e => e.Key)
                .Take(10);

            foreach (var metric in keyMetrics)
            {
                summary.Add($"  - {metric.Key}: {metric.Value} ({metric.Description})");
            }

            return string.Join(Environment.NewLine, summary);
        }
    }
}