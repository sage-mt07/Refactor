using System;

namespace KsqlDsl.Core.Configuration
{
    public interface ICoreSettingsProvider
    {
        CoreSettings GetSettings();
        void UpdateSettings(CoreSettings settings);
        event EventHandler<CoreSettingsChangedEventArgs>? SettingsChanged;
    }
}
