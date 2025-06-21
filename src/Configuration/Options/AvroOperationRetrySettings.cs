using System;
using System.Collections.Generic;

namespace KsqlDsl.Configuration.Options
{
    public class AvroOperationRetrySettings
    {
        public AvroRetryPolicy SchemaRegistration { get; set; } = new()
        {
            MaxAttempts = 3,
            InitialDelay = TimeSpan.FromMilliseconds(500),
            MaxDelay = TimeSpan.FromSeconds(5),
            BackoffMultiplier = 2.0,
            RetryableExceptions = new List<Type> { typeof(System.Net.Http.HttpRequestException), typeof(TimeoutException) },
            NonRetryableExceptions = new List<Type> { typeof(ArgumentException), typeof(InvalidOperationException) }
        };

        public AvroRetryPolicy SchemaRetrieval { get; set; } = new()
        {
            MaxAttempts = 2,
            InitialDelay = TimeSpan.FromMilliseconds(250),
            MaxDelay = TimeSpan.FromSeconds(2),
            BackoffMultiplier = 1.5,
            RetryableExceptions = new List<Type> { typeof(System.Net.Http.HttpRequestException) },
            NonRetryableExceptions = new List<Type> { typeof(ArgumentException) }
        };

        public AvroRetryPolicy CompatibilityCheck { get; set; } = new()
        {
            MaxAttempts = 2,
            InitialDelay = TimeSpan.FromMilliseconds(200),
            MaxDelay = TimeSpan.FromSeconds(1),
            BackoffMultiplier = 1.5,
            RetryableExceptions = new List<Type> { typeof(System.Net.Http.HttpRequestException) },
            NonRetryableExceptions = new List<Type> { typeof(ArgumentException) }
        };
    }
}
