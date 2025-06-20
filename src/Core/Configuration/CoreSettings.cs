using KsqlDsl.Configuration.Abstractions;
using KsqlDsl.Core.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Configuration
{
    public class CoreSettings
    {
        public ValidationMode ValidationMode { get; set; } = ValidationMode.Strict;
        public bool EnableDebugLogging { get; set; } = false;
        public bool EnableEntityCaching { get; set; } = true;
        public bool EnableCircularReferenceCheck { get; set; } = true;
        public bool EnablePropertyValidation { get; set; } = true;
        public int MaxEntityCacheSize { get; set; } = 1000;
        public TimeSpan EntityCacheExpiration { get; set; } = TimeSpan.FromHours(1);

        public CoreSettings Clone()
        {
            return new CoreSettings
            {
                ValidationMode = ValidationMode,
                EnableDebugLogging = EnableDebugLogging,
                EnableEntityCaching = EnableEntityCaching,
                EnableCircularReferenceCheck = EnableCircularReferenceCheck,
                EnablePropertyValidation = EnablePropertyValidation,
                MaxEntityCacheSize = MaxEntityCacheSize,
                EntityCacheExpiration = EntityCacheExpiration
            };
        }

        public void Validate()
        {
            if (MaxEntityCacheSize <= 0)
                throw new CoreConfigurationException("MaxEntityCacheSize must be greater than 0");

            if (EntityCacheExpiration <= TimeSpan.Zero)
                throw new CoreConfigurationException("EntityCacheExpiration must be greater than zero");
        }

        public override string ToString()
        {
            return $"CoreSettings: ValidationMode={ValidationMode}, " +
                   $"Debug={EnableDebugLogging}, Caching={EnableEntityCaching}, " +
                   $"CacheSize={MaxEntityCacheSize}, CacheExpiration={EntityCacheExpiration}";
        }
    }
}
