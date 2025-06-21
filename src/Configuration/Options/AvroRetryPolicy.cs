using System;
using System.Collections.Generic;

namespace KsqlDsl.Configuration.Options
{
    public class AvroRetryPolicy
    {
        public int MaxAttempts { get; set; } = 3;
        public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMilliseconds(500);
        public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(10);
        public double BackoffMultiplier { get; set; } = 2.0;
        public List<Type> RetryableExceptions { get; set; } = new();
        public List<Type> NonRetryableExceptions { get; set; } = new();

        public void Validate()
        {
            if (MaxAttempts <= 0)
                throw new ArgumentException("MaxAttempts must be greater than 0");

            if (InitialDelay <= TimeSpan.Zero)
                throw new ArgumentException("InitialDelay must be positive");

            if (MaxDelay <= TimeSpan.Zero)
                throw new ArgumentException("MaxDelay must be positive");

            if (BackoffMultiplier <= 0)
                throw new ArgumentException("BackoffMultiplier must be positive");
        }
    }
}
