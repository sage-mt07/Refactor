using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Configuration
{
    public class CoreSettingsChangedEventArgs : EventArgs
    {
        public CoreSettings OldSettings { get; }
        public CoreSettings NewSettings { get; }
        public DateTime ChangedAt { get; }

        public CoreSettingsChangedEventArgs(CoreSettings oldSettings, CoreSettings newSettings)
        {
            OldSettings = oldSettings;
            NewSettings = newSettings;
            ChangedAt = DateTime.UtcNow;
        }
    }
}
