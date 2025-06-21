using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Serialization.Abstractions;

public class AvroEntityConfigurationBuilder<T> where T : class
{
    private readonly AvroEntityConfiguration _configuration;

    internal AvroEntityConfigurationBuilder(AvroEntityConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    public AvroEntityConfigurationBuilder<T> ToTopic(string topicName)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException("Topic name cannot be null or empty", nameof(topicName));

        _configuration.TopicName = topicName;
        return this;
    }

    public AvroEntityConfigurationBuilder<T> HasKey<TKey>(System.Linq.Expressions.Expression<Func<T, TKey>> keyExpression)
    {
        if (keyExpression == null)
            throw new ArgumentNullException(nameof(keyExpression));

        var keyProperties = ExtractProperties(keyExpression);
        _configuration.KeyProperties = keyProperties;
        return this;
    }

    public AvroEntityConfigurationBuilder<T> WithPartitions(int partitions)
    {
        if (partitions <= 0)
            throw new ArgumentException("Partitions must be greater than 0", nameof(partitions));

        _configuration.Partitions = partitions;
        return this;
    }

    public AvroEntityConfigurationBuilder<T> WithReplicationFactor(int replicationFactor)
    {
        if (replicationFactor <= 0)
            throw new ArgumentException("ReplicationFactor must be greater than 0", nameof(replicationFactor));

        _configuration.ReplicationFactor = replicationFactor;
        return this;
    }

    public AvroEntityConfigurationBuilder<T> ValidateOnStartup(bool validate = true)
    {
        _configuration.ValidateOnStartup = validate;
        return this;
    }

    public AvroEntityConfigurationBuilder<T> EnableCaching(bool enable = true)
    {
        _configuration.EnableCaching = enable;
        return this;
    }

    public AvroEntityConfigurationBuilder<T> AsStream()
    {
        _configuration.CustomSettings["StreamTableType"] = "Stream";
        return this;
    }

    public AvroEntityConfigurationBuilder<T> AsTable()
    {
        _configuration.CustomSettings["StreamTableType"] = "Table";
        return this;
    }

    public AvroEntityConfiguration Build()
    {
        return _configuration;
    }

    private PropertyInfo[] ExtractProperties<TKey>(System.Linq.Expressions.Expression<Func<T, TKey>> expression)
    {
        if (expression.Body is System.Linq.Expressions.MemberExpression memberExpression)
        {
            if (memberExpression.Member is PropertyInfo property)
            {
                return new[] { property };
            }
        }
        else if (expression.Body is System.Linq.Expressions.NewExpression newExpression)
        {
            var properties = new List<PropertyInfo>();
            foreach (var arg in newExpression.Arguments)
            {
                if (arg is System.Linq.Expressions.MemberExpression argMember && argMember.Member is PropertyInfo prop)
                {
                    properties.Add(prop);
                }
            }
            return properties.ToArray();
        }

        throw new ArgumentException("Invalid key expression", nameof(expression));
    }
}