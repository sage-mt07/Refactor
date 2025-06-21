using KsqlDsl.Configuration;

namespace KsqlDsl.Core.Configuration;

public class CoreSettings
{
    public ValidationMode ValidationMode { get; set; } = ValidationMode.Strict;

    public CoreSettings Clone()
    {
        return new CoreSettings
        {
            ValidationMode = ValidationMode
        };
    }

    public void Validate()
    {
       
    }


}
