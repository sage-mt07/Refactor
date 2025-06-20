using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Configuration
{
    public interface ICoreSettingsProvider
    {
        CoreSettings GetSettings();
        void UpdateSettings(CoreSettings settings);
        event EventHandler<CoreSettingsChangedEventArgs>? SettingsChanged;
    }
}
