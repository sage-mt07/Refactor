using System;

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
