using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Configuration.Abstractions
{
    public class SchemaGenerationOptions
    {
        public string? CustomName { get; set; }
        public string? Namespace { get; set; }
        public string? Documentation { get; set; }
        public bool PrettyFormat { get; set; } = true;
        public bool UseKebabCase { get; set; } = false;
        public bool IncludeDefaultValues { get; set; } = true;
        public bool ValidateOnGeneration { get; set; } = true;

        public SchemaGenerationOptions Clone()
        {
            return new SchemaGenerationOptions
            {
                CustomName = CustomName,
                Namespace = Namespace,
                Documentation = Documentation,
                PrettyFormat = PrettyFormat,
                UseKebabCase = UseKebabCase,
                IncludeDefaultValues = IncludeDefaultValues,
                ValidateOnGeneration = ValidateOnGeneration
            };
        }
    }

}
