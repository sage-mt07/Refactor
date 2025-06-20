using KsqlDsl.Core.Abstractions;
using KsqlDsl.Core.Validation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace KsqlDsl.Core
{
    /// <summary>
    /// Core層設計制約の検証
    /// 設計理由：依存関係の一方向性確保
    /// </summary>
    public static class CoreLayerValidation
    {
        private static readonly string[] ForbiddenNamespaces = new[]
        {
            "KsqlDsl.Communication",
            "KsqlDsl.Messaging",
            "KsqlDsl.Serialization",
            "KsqlDsl.Monitoring",
            "KsqlDsl.Avro",
            "KsqlDsl.Services"
        };

        public static CoreValidationResult ValidateCoreDependencies()
        {
            var result = new CoreValidationResult { IsValid = true };
            var coreAssembly = typeof(IKafkaContext).Assembly;
            var coreTypes = coreAssembly.GetTypes()
                .Where(t => t.Namespace?.StartsWith("KsqlDsl.Core") == true);

            foreach (var type in coreTypes)
            {
                ValidateTypedependencies(type, result);
            }

            return result;
        }

        private static void ValidateTypedependencies(Type type, CoreValidationResult result)
        {
            var dependencies = type.GetReferencedTypes();

            foreach (var dependency in dependencies)
            {
                if (dependency.Namespace != null &&
                    ForbiddenNamespaces.Any(ns => dependency.Namespace.StartsWith(ns)))
                {
                    result.IsValid = false;
                    result.Errors.Add(
                        $"Core type {type.Name} has forbidden dependency on {dependency.Namespace}.{dependency.Name}");
                }
            }
        }

        private static IEnumerable<Type> GetReferencedTypes(this Type type)
        {
            var referencedTypes = new HashSet<Type>();

            // Field types
            foreach (var field in type.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
            {
                referencedTypes.Add(field.FieldType);
            }

            // Property types
            foreach (var property in type.GetProperties())
            {
                referencedTypes.Add(property.PropertyType);
            }

            // Method parameter and return types
            foreach (var method in type.GetMethods())
            {
                referencedTypes.Add(method.ReturnType);
                foreach (var param in method.GetParameters())
                {
                    referencedTypes.Add(param.ParameterType);
                }
            }

            return referencedTypes.Where(t => t.Assembly != type.Assembly);
        }


    }
}
