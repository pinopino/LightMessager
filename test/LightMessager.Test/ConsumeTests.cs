using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;

namespace LightMessager.Test
{
    public class ConsumeTests
    {
        private IConnection _connection;

        public ConsumeTests()
        {
            var factory = new ConnectionFactory() { HostName = "localhost", Port = 5673 };
            _connection = factory.CreateConnection();
        }

        [Fact]
        public void 直接消费消息()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var orders = new List<Order>();
            for (var i = 0; i < 5; i++)
            {
                var item = new Order()
                {
                    OrderId = i.ToString(),
                    Price = 100M,
                    ProductCode = "Computer",
                    Quantity = 10,
                    CreatedTime = DateTime.Now
                };
                orders.Add(item);
            }
            var channel = _connection.CreateModel();
            mqHub.Send(orders);

            var recvOrders = new List<Order>();
            mqHub.Consume<Order>(msg =>
            {
                recvOrders.Add(msg);
            });

            Thread.Sleep(3000);

            Assert.Equal(orders.Count, recvOrders.Count);
            Assert.Equal(orders[0].OrderId, recvOrders[0].OrderId);
            Assert.Equal(orders[0].Price, recvOrders[0].Price);
            Assert.Equal(orders[0].ProductCode, recvOrders[0].ProductCode);
            Assert.Equal(orders[0].Quantity, recvOrders[0].Quantity);
            Assert.Equal(orders[0].CreatedTime, recvOrders[0].CreatedTime);

            // clean up
            channel.QueueDelete("Order");
        }

        private IConfigurationRoot GetConfig()
        {
            var builder = new ConfigurationBuilder()
                .AddJsonFile("config/appsettings.json");

            return builder.Build();
        }
    }
}
