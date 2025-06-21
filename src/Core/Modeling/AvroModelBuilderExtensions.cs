using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Modeling
{
    public static class AvroModelBuilderExtensions
    {
        /// <summary>
        /// 設定済みエンティティの検証
        /// </summary>
        public static AvroModelBuilder ValidateEntities(this AvroModelBuilder builder)
        {
            var validationResult = builder.ValidateModel();
            if (validationResult.HasErrors)
            {
                throw new InvalidOperationException($"Model validation failed:{Environment.NewLine}{validationResult.GetSummary()}");
            }
            return builder;
        }

        /// <summary>
        /// 条件付きエンティティ設定
        /// </summary>
        public static AvroModelBuilder EntityIf<T>(this AvroModelBuilder builder, bool condition, Action<AvroEntityTypeBuilder<T>> configure) where T : class
        {
            if (condition)
            {
                var entityBuilder = builder.Entity<T>();
                configure(entityBuilder);
            }
            return builder;
        }

        /// <summary>
        /// 複数エンティティの一括設定
        /// </summary>
        public static AvroModelBuilder Entities(this AvroModelBuilder builder, params Type[] entityTypes)
        {
            foreach (var entityType in entityTypes)
            {
                var method = typeof(AvroModelBuilder).GetMethod(nameof(AvroModelBuilder.Entity))!.MakeGenericMethod(entityType);
                method.Invoke(builder, null);
            }
            return builder;
        }

        /// <summary>
        /// アセンブリからエンティティ自動発見
        /// </summary>
        public static AvroModelBuilder DiscoverEntitiesFromAssembly(this AvroModelBuilder builder, Assembly assembly)
        {
            var entityTypes = assembly.GetTypes()
                .Where(t => t.IsClass && !t.IsAbstract && t.GetCustomAttribute<TopicAttribute>() != null)
                .ToArray();

            return builder.Entities(entityTypes);
        }

        /// <summary>
        /// 現在のアセンブリからエンティティ自動発見
        /// </summary>
        public static AvroModelBuilder DiscoverEntitiesFromCurrentAssembly(this AvroModelBuilder builder)
        {
            return builder.DiscoverEntitiesFromAssembly(Assembly.GetCallingAssembly());
        }
    }
}
