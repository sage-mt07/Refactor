using KsqlDsl.Attributes;
using KsqlDsl.Configuration.Builders;
using KsqlDsl.Core.Modeling;
using Xunit;

namespace KsqlDsl.Tests
{
    [Topic("mapped-topic")]
    public class MappedEntity
    {
        [Key]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    public class MappingKafkaContext : KafkaContext
    {
        public EventSet<MappedEntity> MappedEntities => Set<MappedEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Event<MappedEntity>();
        }

        protected override void OnConfiguring(KafkaContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseKafka("localhost:9092");
        }
    }

    public class PocoMappingTests
    {
        [Fact]
        public void TopicAttribute_Should_BeConverted_ToKafkaTopicName()
        {
            using var context = new MappingKafkaContext();

            var topicName = context.MappedEntities.GetTopicName();

            Assert.Equal("mapped-topic", topicName);
        }

        [Fact]
        public void KeyAttribute_Should_Map_ToPartitionKey()
        {
            using var context = new MappingKafkaContext();

            var entityModel = context.MappedEntities.GetEntityModel();

            Assert.Single(entityModel.KeyProperties);
            Assert.Equal(nameof(MappedEntity.Id), entityModel.KeyProperties[0].Name);
        }
    }
}
