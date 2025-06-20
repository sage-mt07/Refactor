using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Integration
{
    public class LayerIntegrationBridge : ILayerIntegrationBridge
    {
        private readonly ICoreIntegrationService _coreService;

        public LayerIntegrationBridge(ICoreIntegrationService coreService)
        {
            _coreService = coreService ?? throw new ArgumentNullException(nameof(coreService));
        }

        public async Task<bool> ValidateMessagingIntegrationAsync()
        {
            // Messaging層が正しく分離されているかチェック
            try
            {
                // Producer/Consumer/MessageBus機能の統合確認
                var hasMessagingTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .Any(a => a.GetTypes().Any(t =>
                        t.Namespace?.StartsWith("KsqlDsl.Messaging") == true));

                await Task.CompletedTask;
                return hasMessagingTypes;
            }
            catch
            {
                return false;
            }
        }

        public async Task<bool> ValidateSerializationIntegrationAsync()
        {
            // Serialization層（Avro/JSON）の統合確認
            try
            {
                var hasSerializationTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .Any(a => a.GetTypes().Any(t =>
                        t.Namespace?.StartsWith("KsqlDsl.Serialization") == true));

                await Task.CompletedTask;
                return hasSerializationTypes;
            }
            catch
            {
                return false;
            }
        }

        public async Task<bool> ValidateMonitoringIntegrationAsync()
        {
            // Monitoring層（Health/Metrics/Diagnostics）の統合確認
            try
            {
                var hasMonitoringTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .Any(a => a.GetTypes().Any(t =>
                        t.Namespace?.StartsWith("KsqlDsl.Monitoring") == true));

                await Task.CompletedTask;
                return hasMonitoringTypes;
            }
            catch
            {
                return false;
            }
        }

        public async Task<bool> ValidateQueryIntegrationAsync()
        {
            // Query層（LINQ/KSQL/EventSets）の統合確認
            try
            {
                var hasQueryTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .Any(a => a.GetTypes().Any(t =>
                        t.Namespace?.StartsWith("KsqlDsl.Query") == true));

                await Task.CompletedTask;
                return hasQueryTypes;
            }
            catch
            {
                return false;
            }
        }

        public async Task<bool> ValidateConfigurationIntegrationAsync()
        {
            // Configuration層（Options/Validation/Overrides）の統合確認
            try
            {
                var hasConfigTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .Any(a => a.GetTypes().Any(t =>
                        t.Namespace?.StartsWith("KsqlDsl.Configuration") == true));

                await Task.CompletedTask;
                return hasConfigTypes;
            }
            catch
            {
                return false;
            }
        }

        public async Task<LayerIntegrationReport> GetIntegrationReportAsync()
        {
            var report = new LayerIntegrationReport
            {
                GeneratedAt = DateTime.UtcNow
            };

            try
            {
                // 各層の統合状態をチェック
                report.MessagingIntegration = await ValidateMessagingIntegrationAsync();
                report.SerializationIntegration = await ValidateSerializationIntegrationAsync();
                report.MonitoringIntegration = await ValidateMonitoringIntegrationAsync();
                report.QueryIntegration = await ValidateQueryIntegrationAsync();
                report.ConfigurationIntegration = await ValidateConfigurationIntegrationAsync();

                // Core層の状態取得
                var coreHealth = await _coreService.GetHealthReportAsync();
                report.CoreHealth = coreHealth.Status;

                // 全体的な統合状態判定
                report.OverallIntegration =
                    report.MessagingIntegration &&
                    report.SerializationIntegration &&
                    report.MonitoringIntegration &&
                    report.QueryIntegration &&
                    report.ConfigurationIntegration &&
                    coreHealth.Status == CoreHealthStatus.Healthy;

                // 統計情報
                var coreDiagnostics = _coreService.GetDiagnostics();
                report.Statistics = new Dictionary<string, object>
                {
                    ["RegisteredEntities"] = coreDiagnostics.Statistics.GetValueOrDefault("TotalModels", 0),
                    ["ValidModels"] = coreDiagnostics.Statistics.GetValueOrDefault("ValidModels", 0),
                    ["LoadedAssemblies"] = AppDomain.CurrentDomain.GetAssemblies().Length
                };

                // 問題の識別
                if (!report.OverallIntegration)
                {
                    if (!report.MessagingIntegration)
                        report.Issues.Add("Messaging layer integration failed");
                    if (!report.SerializationIntegration)
                        report.Issues.Add("Serialization layer integration failed");
                    if (!report.MonitoringIntegration)
                        report.Issues.Add("Monitoring layer integration failed");
                    if (!report.QueryIntegration)
                        report.Issues.Add("Query layer integration failed");
                    if (!report.ConfigurationIntegration)
                        report.Issues.Add("Configuration layer integration failed");
                    if (coreHealth.Status != CoreHealthStatus.Healthy)
                        report.Issues.Add($"Core layer health is {coreHealth.Status}");
                }
            }
            catch (Exception ex)
            {
                report.OverallIntegration = false;
                report.Issues.Add($"Integration check failed: {ex.Message}");
            }

            return report;
        }
    }
}
