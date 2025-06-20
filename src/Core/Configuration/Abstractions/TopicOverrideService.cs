using System;
using System.Collections.Generic;

namespace KsqlDsl.Core.Configuration.Abstractions
{
    public class TopicOverrideService
    {
        private readonly Dictionary<string, string> _overrides = new();

        public void AddOverride(string originalTopic, string overrideTopic)
        {
            _overrides[originalTopic] = overrideTopic;
        }

        public string GetOverrideTopic(string originalTopic)
        {
            return _overrides.TryGetValue(originalTopic, out var overrideTopic)
                ? overrideTopic
                : originalTopic;
        }

        public Dictionary<string, string> GetAllOverrides()
        {
            return new Dictionary<string, string>(_overrides);
        }

        public string GetOverrideSummary()
        {
            if (_overrides.Count == 0)
                return "Topic Overrides: なし";

            var summary = new List<string> { "Topic Overrides:" };
            foreach (var kvp in _overrides)
            {
                summary.Add($"  {kvp.Key} → {kvp.Value}");
            }
            return string.Join(Environment.NewLine, summary);
        }
    }
}
