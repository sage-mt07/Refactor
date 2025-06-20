using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace KsqlDsl.Core.Integration
{
    public class LayerIntegrationReport
    {
        public DateTime GeneratedAt { get; set; }
        public bool OverallIntegration { get; set; }
        public CoreHealthStatus CoreHealth { get; set; }

        // 各層の統合状態
        public bool MessagingIntegration { get; set; }
        public bool SerializationIntegration { get; set; }
        public bool MonitoringIntegration { get; set; }
        public bool QueryIntegration { get; set; }
        public bool ConfigurationIntegration { get; set; }

        public List<string> Issues { get; set; } = new();
        public Dictionary<string, object> Statistics { get; set; } = new();

        public string GetSummary()
        {
            var status = OverallIntegration ? "✅ SUCCESS" : "❌ FAILED";
            var issueCount = Issues.Count;
            var layerCount = GetSuccessfulLayerCount();

            return $"Layer Integration: {status} ({layerCount}/5 layers), " +
                   $"Core: {CoreHealth}, Issues: {issueCount}";
        }

        private int GetSuccessfulLayerCount()
        {
            var layers = new[]
            {
                MessagingIntegration,
                SerializationIntegration,
                MonitoringIntegration,
                QueryIntegration,
                ConfigurationIntegration
            };
            return layers.Count(layer => layer);
        }
    }
}
