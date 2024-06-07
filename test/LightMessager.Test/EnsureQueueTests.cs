using LightMessager.Model;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;

namespace LightMessager.Test
{
    public class EnsureQueueTests
    {
        private IConnection _connection;

        public EnsureQueueTests()
        {
            var factory = new ConnectionFactory() { HostName = "localhost", Port = 5673 };
            _connection = factory.CreateConnection();
        }

        [Fact]
        public void EnsureSendQueue_queue不存在时自动创建queue1()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            using (var channel = _connection.CreateModel())
            {
                mqHub.EnsureSend(channel, typeof(Order), 0, out QueueInfo info, out _);
                Assert.Equal(string.Empty, info.Exchange);
                Assert.Equal(typeof(Order).Name, info.Queue);

                var res = channel.QueueDeclarePassive(info.Queue);
                Assert.Equal(info.Queue, res.QueueName);

                mqHub.EnsureSendQueue(channel, typeof(Order), out info);
                var cache = mqHub.GetMetaInfoCache();
                Assert.Equal(1, cache.Count);
                Assert.True(cache.TryGetValue(info.Queue, out QueueInfo cachedInfo));
                Assert.Equal(info.Exchange, cachedInfo.Exchange);
                Assert.Equal(info.Queue, cachedInfo.Queue);

                // clean up
                channel.QueueDelete(info.Queue);
            }
        }

        [Fact]
        public void EnsureSendQueue_queue不存在时自动创建queue2()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            using (var channel = _connection.CreateModel())
            {
                mqHub.EnsureSend(channel, typeof(Order), 10, out QueueInfo info, out DelayQueueInfo delayInfo);
                Assert.Equal(string.Empty, info.Exchange);
                Assert.Equal(typeof(Order).Name, info.Queue);

                var res = channel.QueueDeclarePassive(delayInfo.DelayQueue);
                Assert.Equal(delayInfo.DelayQueue, res.QueueName);

                var cache = mqHub.GetMetaInfoCache();
                Assert.True(cache.TryGetValue(info.Queue, out _));
                Assert.True(cache.TryGetValue(delayInfo.DelayQueue, out _));

                // clean up
                channel.QueueDelete(info.Queue);
                channel.QueueDelete(delayInfo.DelayQueue);
            }

        }

        [Fact]
        public void EnsureRouteQueue_exchange不存在时自动创建exchange1()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            using (var channel = _connection.CreateModel())
            {
                mqHub.EnsureRoute(channel, typeof(Order), "computer", 0, out QueueInfo info, out _);
                Assert.Equal(string.Empty, info.Queue);

                channel.ExchangeDeclarePassive(info.Exchange);

                // clean up
                channel.ExchangeDelete(info.Exchange);
            }
        }

        [Fact]
        public void EnsureRouteQueue_exchange不存在时自动创建exchange2()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            using (var channel = _connection.CreateModel())
            {
                mqHub.EnsureRoute(channel, typeof(Order), "computer", 10, out QueueInfo info, out DelayQueueInfo delayInfo);
                Assert.Equal(string.Empty, info.Queue);

                channel.ExchangeDeclarePassive(info.Exchange);

                var res = channel.QueueDeclarePassive(delayInfo.DelayQueue);
                Assert.Equal(delayInfo.DelayQueue, res.QueueName);

                // clean up
                channel.ExchangeDelete(info.Exchange);
                channel.QueueDelete(delayInfo.DelayQueue);
            }
        }

        [Fact]
        public void EnsurePublishQueue_exchange不存在时自动创建exchange1()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            using (var channel = _connection.CreateModel())
            {
                mqHub.EnsurePublish(channel, typeof(Order), 0, out QueueInfo info, out _);
                Assert.Equal(string.Empty, info.Queue);

                channel.ExchangeDeclarePassive(info.Exchange);

                // clean up
                channel.ExchangeDelete(info.Exchange);
            }
        }

        [Fact]
        public void EnsurePublishQueue_exchange不存在时自动创建exchange2()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            using (var channel = _connection.CreateModel())
            {
                mqHub.EnsurePublish(channel, typeof(Order), 10, out QueueInfo info, out DelayQueueInfo delayInfo);
                Assert.Equal(string.Empty, info.Queue);

                channel.ExchangeDeclarePassive(info.Exchange);

                var res = channel.QueueDeclarePassive(delayInfo.DelayQueue);
                Assert.Equal(delayInfo.DelayQueue, res.QueueName);

                // clean up
                channel.ExchangeDelete(info.Exchange);
                channel.QueueDelete(delayInfo.DelayQueue);
            }
        }

        private IConfigurationRoot GetConfig()
        {
            var builder = new ConfigurationBuilder()
                .AddJsonFile("config/appsettings.json");

            return builder.Build();
        }
    }
}
