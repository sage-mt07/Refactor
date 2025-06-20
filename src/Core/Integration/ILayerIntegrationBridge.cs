using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Integration
{
    public interface ILayerIntegrationBridge
    {
        // Messaging層との統合
        Task<bool> ValidateMessagingIntegrationAsync();

        // Serialization層との統合  
        Task<bool> ValidateSerializationIntegrationAsync();

        // Monitoring層との統合
        Task<bool> ValidateMonitoringIntegrationAsync();

        // Query層との統合
        Task<bool> ValidateQueryIntegrationAsync();

        // Configuration層との統合
        Task<bool> ValidateConfigurationIntegrationAsync();
    }

}
