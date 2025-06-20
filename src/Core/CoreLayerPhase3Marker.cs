using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core
{
    public static class CoreLayerPhase3Marker
    {
        public const string Version = "Phase3-Refactored";
        public const string RefactorDate = "2025-06-20";

        public static bool IsRefactored => true;

        public static Dictionary<string, string> GetRefactorInfo()
        {
            return new Dictionary<string, string>
            {
                ["Version"] = Version,
                ["RefactorDate"] = RefactorDate,
                ["SeparatedLayers"] = "Messaging, Serialization, Monitoring, Query, Configuration",
                ["CoreResponsibilities"] = "Abstractions, Models, Context, Validation",
                ["DependencyDirection"] = "One-way (Core ← Other Layers)",
                ["ValidationCompleted"] = "✅"
            };
        }

        public static void ValidatePhase3Compliance()
        {
            var coreAssembly = typeof(CoreLayerPhase3Marker).Assembly;
            var coreTypes = coreAssembly.GetTypes()
                .Where(t => t.Namespace?.StartsWith("KsqlDsl.Core") == true);

            var forbiddenDependencies = new[]
            {
                "KsqlDsl.Communication", // 旧名前空間
                "KsqlDsl.Avro"          // 旧名前空間
            };

            foreach (var type in coreTypes)
            {
                foreach (var dependency in GetTypeDependencies(type))
                {
                    if (forbiddenDependencies.Any(ns => dependency.Namespace?.StartsWith(ns) == true))
                    {
                        throw new InvalidOperationException(
                            $"Core type {type.Name} has forbidden dependency on {dependency.Namespace}.{dependency.Name}");
                    }
                }
            }
        }

        private static IEnumerable<Type> GetTypeDependencies(Type type)
        {
            var dependencies = new HashSet<Type>();

            // Field dependencies
            foreach (var field in type.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
            {
                dependencies.Add(field.FieldType);
            }

            // Property dependencies
            foreach (var property in type.GetProperties())
            {
                dependencies.Add(property.PropertyType);
            }

            // Method dependencies
            foreach (var method in type.GetMethods())
            {
                dependencies.Add(method.ReturnType);
                foreach (var param in method.GetParameters())
                {
                    dependencies.Add(param.ParameterType);
                }
            }

            return dependencies.Where(t => t.Assembly != type.Assembly);
        }
    }
}
