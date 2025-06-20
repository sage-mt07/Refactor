namespace KsqlDsl.Messaging.Abstractions.Configuration
{
    public interface ICommonConfiguration
    {
        string BootstrapServers { get; }
        string ClientId { get; }
    }
}
