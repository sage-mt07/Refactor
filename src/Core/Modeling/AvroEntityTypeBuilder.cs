using KsqlDsl.Application;
using KsqlDsl.Core.Abstractions;
using KsqlDsl.Serialization.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Modeling
{
    public class AvroEntityTypeBuilder<T> where T : class
    {
        private readonly AvroEntityConfiguration _configuration;
        private readonly AvroModelBuilder _modelBuilder;

        internal AvroEntityTypeBuilder(AvroEntityConfiguration configuration, AvroModelBuilder modelBuilder)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _modelBuilder = modelBuilder ?? throw new ArgumentNullException(nameof(modelBuilder));
        }

        /// <summary>
        /// トピック名を設定
        /// </summary>
        public AvroEntityTypeBuilder<T> ToTopic(string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName))
                throw new ArgumentException("Topic name cannot be null or empty", nameof(topicName));

            _configuration.TopicName = topicName;
            return this;
        }

        /// <summary>
        /// キープロパティを設定
        /// </summary>
        public AvroEntityTypeBuilder<T> HasKey<TKey>(Expression<Func<T, TKey>> keyExpression)
        {
            if (keyExpression == null)
                throw new ArgumentNullException(nameof(keyExpression));

            var keyProperties = ExtractProperties(keyExpression);
            _configuration.KeyProperties = keyProperties;
            return this;
        }

        /// <summary>
        /// パーティション数を設定
        /// </summary>
        public AvroEntityTypeBuilder<T> WithPartitions(int partitions)
        {
            if (partitions <= 0)
                throw new ArgumentException("Partitions must be greater than 0", nameof(partitions));

            _configuration.Partitions = partitions;
            return this;
        }

        /// <summary>
        /// レプリケーションファクターを設定
        /// </summary>
        public AvroEntityTypeBuilder<T> WithReplicationFactor(int replicationFactor)
        {
            if (replicationFactor <= 0)
                throw new ArgumentException("ReplicationFactor must be greater than 0", nameof(replicationFactor));

            _configuration.ReplicationFactor = replicationFactor;
            return this;
        }

        /// <summary>
        /// 起動時検証を設定
        /// </summary>
        public AvroEntityTypeBuilder<T> ValidateOnStartup(bool validate = true)
        {
            _configuration.ValidateOnStartup = validate;
            return this;
        }

        /// <summary>
        /// キャッシュを有効化
        /// </summary>
        public AvroEntityTypeBuilder<T> EnableCaching(bool enable = true)
        {
            _configuration.EnableCaching = enable;
            return this;
        }

        /// <summary>
        /// Streamとして設定
        /// </summary>
        public AvroEntityTypeBuilder<T> AsStream()
        {
            _configuration.CustomSettings["StreamTableType"] = "Stream";
            return this;
        }

        /// <summary>
        /// Tableとして設定
        /// </summary>
        public AvroEntityTypeBuilder<T> AsTable()
        {
            _configuration.CustomSettings["StreamTableType"] = "Table";
            return this;
        }

        /// <summary>
        /// プロパティ設定を開始
        /// </summary>
        public AvroPropertyBuilder<T, TProperty> Property<TProperty>(
            Expression<Func<T, TProperty>> propertyExpression)
        {
            if (propertyExpression == null)
                throw new ArgumentNullException(nameof(propertyExpression));

            var property = ExtractProperty(propertyExpression);
            return new AvroPropertyBuilder<T, TProperty>(this, property);
        }

        /// <summary>
        /// 属性ベースの設定を使用
        /// </summary>
        public AvroEntityTypeBuilder<T> UseAttributeBasedConfiguration()
        {
            // 属性情報は既にコンストラクタで適用済み
            return this;
        }

        /// <summary>
        /// デフォルトのトピック名を使用
        /// </summary>
        public AvroEntityTypeBuilder<T> UseDefaultTopic()
        {
            var entityType = typeof(T);
            var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();

            if (topicAttribute != null)
            {
                ToTopic(topicAttribute.TopicName);
            }
            else
            {
                ToTopic(entityType.Name.ToLowerInvariant());
            }

            return this;
        }

        /// <summary>
        /// 属性からキー設定を使用
        /// </summary>
        public AvroEntityTypeBuilder<T> UseKeyFromAttribute()
        {
            var entityType = typeof(T);
            var keyProperties = entityType.GetProperties()
                .Where(p => p.GetCustomAttribute<KeyAttribute>() != null)
                .OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0)
                .ToArray();

            if (keyProperties.Length > 0)
            {
                _configuration.KeyProperties = keyProperties;
            }

            return this;
        }

        /// <summary>
        /// カスタム設定を追加
        /// </summary>
        public AvroEntityTypeBuilder<T> WithCustomSetting(string key, object value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", nameof(key));

            _configuration.CustomSettings[key] = value;
            return this;
        }

        /// <summary>
        /// ModelBuilderへ戻る
        /// </summary>
        public AvroModelBuilder Model => _modelBuilder;

        #region Private Methods

        /// <summary>
        /// 式からプロパティを抽出
        /// </summary>
        private PropertyInfo[] ExtractProperties<TKey>(Expression<Func<T, TKey>> expression)
        {
            if (expression.Body is MemberExpression memberExpression)
            {
                if (memberExpression.Member is PropertyInfo property)
                {
                    return new[] { property };
                }
            }
            else if (expression.Body is NewExpression newExpression)
            {
                var properties = new List<PropertyInfo>();
                foreach (var arg in newExpression.Arguments)
                {
                    if (arg is MemberExpression argMember && argMember.Member is PropertyInfo prop)
                    {
                        properties.Add(prop);
                    }
                }
                return properties.ToArray();
            }

            throw new ArgumentException("Invalid key expression", nameof(expression));
        }

        /// <summary>
        /// 単一プロパティを抽出
        /// </summary>
        private PropertyInfo ExtractProperty<TProperty>(Expression<Func<T, TProperty>> expression)
        {
            if (expression.Body is MemberExpression memberExpression)
            {
                if (memberExpression.Member is PropertyInfo property)
                {
                    return property;
                }
            }

            throw new ArgumentException("Invalid property expression", nameof(expression));
        }

        #endregion
    }
}
