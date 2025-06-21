using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace KsqlDsl.Application
{
    public class AvroModelBuilder
    {
        private readonly Dictionary<Type, AvroEntityConfiguration> _configurations = new();

        public AvroEntityTypeBuilder<T> Entity<T>() where T : class
        {
            var entityType = typeof(T);
            if (!_configurations.ContainsKey(entityType))
            {
                _configurations[entityType] = new AvroEntityConfiguration(entityType);
            }
            return new AvroEntityTypeBuilder<T>(_configurations[entityType]);
        }

        public IReadOnlyDictionary<Type, AvroEntityConfiguration> Build()
        {
            return _configurations;
        }

        public int EntityCount => _configurations.Count;

        public bool HasEntity<T>() where T : class
        {
            return _configurations.ContainsKey(typeof(T));
        }

        public bool HasEntity(Type entityType)
        {
            return _configurations.ContainsKey(entityType);
        }
    }

    public class AvroEntityTypeBuilder<T> where T : class
    {
        private readonly AvroEntityConfiguration _configuration;

        internal AvroEntityTypeBuilder(AvroEntityConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public AvroEntityTypeBuilder<T> ToTopic(string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName))
                throw new ArgumentException("Topic name cannot be null or empty", nameof(topicName));

            _configuration.TopicName = topicName;
            return this;
        }

        public AvroEntityTypeBuilder<T> HasKey<TKey>(Expression<Func<T, TKey>> keyExpression)
        {
            if (keyExpression == null)
                throw new ArgumentNullException(nameof(keyExpression));

            var keyProperties = ExtractProperties(keyExpression);
            _configuration.KeyProperties = keyProperties;
            return this;
        }

        public AvroPropertyBuilder<T, TProperty> Property<TProperty>(
            Expression<Func<T, TProperty>> propertyExpression)
        {
            if (propertyExpression == null)
                throw new ArgumentNullException(nameof(propertyExpression));

            var property = ExtractProperty(propertyExpression);
            return new AvroPropertyBuilder<T, TProperty>(this, property);
        }

        public AvroEntityTypeBuilder<T> ValidateOnStartup(bool validate = true)
        {
            _configuration.ValidateOnStartup = validate;
            return this;
        }

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
    }

    public class AvroPropertyBuilder<T, TProperty> where T : class
    {
        private readonly AvroEntityTypeBuilder<T> _entityBuilder;
        private readonly PropertyInfo _property;

        internal AvroPropertyBuilder(AvroEntityTypeBuilder<T> entityBuilder, PropertyInfo property)
        {
            _entityBuilder = entityBuilder ?? throw new ArgumentNullException(nameof(entityBuilder));
            _property = property ?? throw new ArgumentNullException(nameof(property));
        }

        public AvroPropertyBuilder<T, TProperty> IsRequired(bool required = true)
        {
            return this;
        }

        public AvroPropertyBuilder<T, TProperty> HasMaxLength(int maxLength)
        {
            if (maxLength <= 0)
                throw new ArgumentException("Max length must be greater than 0", nameof(maxLength));

            return this;
        }

        public AvroPropertyBuilder<T, TProperty> HasPrecision(int precision, int scale)
        {
            if (precision <= 0)
                throw new ArgumentException("Precision must be greater than 0", nameof(precision));
            if (scale < 0 || scale > precision)
                throw new ArgumentException("Scale must be between 0 and precision", nameof(scale));

            return this;
        }

        public AvroPropertyBuilder<T, TProperty> HasDefaultValue(TProperty defaultValue)
        {
            return this;
        }

        public AvroPropertyBuilder<T, TProperty> IsIgnored(bool ignored = true)
        {
            return this;
        }

        public AvroEntityTypeBuilder<T> Entity()
        {
            return _entityBuilder;
        }

        public static implicit operator AvroEntityTypeBuilder<T>(AvroPropertyBuilder<T, TProperty> propertyBuilder)
        {
            return propertyBuilder._entityBuilder;
        }
    }
}