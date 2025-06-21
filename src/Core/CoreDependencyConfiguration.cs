using KsqlDsl.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace KsqlDsl.Core
{
    /// <summary>
    /// Core層の依存関係設定
    /// 設計理由：Core層の抽象定義と実装の分離
    /// </summary>
    public static class CoreDependencyConfiguration
    {
        public static IServiceCollection AddKsqlDslCore(this IServiceCollection services)
        {
        
            return services;
        }


        public static void ValidateCoreLayerDependencies()
        {
            var coreTypes = new[]
            {
                typeof(IKafkaContext),
                typeof(IEntitySet<>),
                typeof(ISerializationManager<>)

            };

            foreach (var type in coreTypes)
            {
                if (type.Assembly != typeof(CoreDependencyConfiguration).Assembly)
                {
                    throw new InvalidOperationException(
                        $"Core layer type {type.Name} must be defined in Core assembly");
                }
            }
        }
    }
}