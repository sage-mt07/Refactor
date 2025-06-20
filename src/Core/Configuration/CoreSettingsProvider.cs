using System;

namespace KsqlDsl.Core.Configuration
{
    public class CoreSettingsProvider : ICoreSettingsProvider
    {
        private CoreSettings _settings = new();
        private readonly object _lock = new();

        public event EventHandler<CoreSettingsChangedEventArgs>? SettingsChanged;

        public CoreSettings GetSettings()
        {
            lock (_lock)
            {
                return _settings.Clone();
            }
        }

        public void UpdateSettings(CoreSettings settings)
        {
            if (settings == null)
                throw new ArgumentNullException(nameof(settings));

            settings.Validate();

            CoreSettings oldSettings;
            lock (_lock)
            {
                oldSettings = _settings.Clone();
                _settings = settings.Clone();
            }

            SettingsChanged?.Invoke(this, new CoreSettingsChangedEventArgs(oldSettings, settings));
        }
    }
}
