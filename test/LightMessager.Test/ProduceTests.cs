using LightMessager.Model;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace LightMessager.Test
{
    public class ProduceTests
    {
        private IConnection _connection;

        public ProduceTests()
        {
            var factory = new ConnectionFactory() { HostName = "localhost", Port = 5673 };
            _connection = factory.CreateConnection();
        }

        [Fact]
        public void 直发单条消息1()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var order = new Order
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            mqHub.Send(order);

            var json = string.Empty;
            var channel = _connection.CreateModel();
            mqHub.EnsureSendQueue(channel, typeof(Order), out QueueInfo info);
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                json = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(info.Queue, false, consumer);

            Thread.Sleep(1000 * 2);

            var msg = JsonConvert.DeserializeObject<Order>(json);
            Assert.Equal(order.OrderId, msg.OrderId);
            Assert.Equal(order.Price, msg.Price);
            Assert.Equal(order.ProductCode, msg.ProductCode);
            Assert.Equal(order.Quantity, msg.Quantity);
            Assert.Equal(order.CreatedTime, msg.CreatedTime);

            // clean up
            channel.QueueDelete(info.Queue);
        }

        [Fact]
        public void 直发单条消息延时()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var order = new Order
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            mqHub.Send(order, delay: 5);

            var json = string.Empty;
            var channel = _connection.CreateModel();
            mqHub.EnsureSendQueue(channel, typeof(Order), out QueueInfo info);
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                json = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(info.Queue, false, consumer);

            Thread.Sleep(1000 * 5);

            var msg = JsonConvert.DeserializeObject<Order>(json);
            Assert.Equal(order.OrderId, msg.OrderId);
            Assert.Equal(order.Price, msg.Price);
            Assert.Equal(order.ProductCode, msg.ProductCode);
            Assert.Equal(order.Quantity, msg.Quantity);
            Assert.Equal(order.CreatedTime, msg.CreatedTime);

            Assert.True((DateTime.Now - msg.CreatedTime).Seconds >= 5);

            // clean up
            channel.QueueDelete(info.Queue);
        }

        [Fact]
        public void 发送单条消息延时带RouteKey1()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var order = new Order
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 5,
                CreatedTime = DateTime.Now
            };
            mqHub.Send(order, routeKey: "computer", delay: 5);

            var json = string.Empty;
            var channel = _connection.CreateModel();
            mqHub.EnsureRoute(channel, typeof(Order), "computer", 5, out QueueInfo info, out DelayQueueInfo delayInfo);
            var res = channel.QueueDeclare();
            channel.QueueBind(res.QueueName, info.Exchange, "computer");
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                json = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(res.QueueName, false, consumer);

            Thread.Sleep(1000 * 5);

            var msg = JsonConvert.DeserializeObject<Order>(json);
            Assert.Equal(order.OrderId, msg.OrderId);
            Assert.Equal(order.Price, msg.Price);
            Assert.Equal(order.ProductCode, msg.ProductCode);
            Assert.Equal(order.Quantity, msg.Quantity);
            Assert.Equal(order.CreatedTime, msg.CreatedTime);

            Assert.True((DateTime.Now - msg.CreatedTime).Seconds >= 5);

            //clean up
            channel.QueueDelete(delayInfo.DelayQueue);
            channel.ExchangeDelete(info.Exchange);
        }

        [Fact]
        public void 发送单条消息延时带RouteKey2()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var order = new Order
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 2000M,
                ProductCode = "Office Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            mqHub.Send(order, routeKey: "office.expensive", delay: 5);

            var json = string.Empty;
            var channel = _connection.CreateModel();
            mqHub.EnsureTopic(channel, typeof(Order), "office.expensive", 5, out QueueInfo info, out DelayQueueInfo delayInfo);
            var res = channel.QueueDeclare();
            channel.QueueBind(res.QueueName, info.Exchange, "office.*");
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                json = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(res.QueueName, false, consumer);

            Thread.Sleep(1000 * 5);

            var msg = JsonConvert.DeserializeObject<Order>(json);
            Assert.Equal(order.OrderId, msg.OrderId);
            Assert.Equal(order.Price, msg.Price);
            Assert.Equal(order.ProductCode, msg.ProductCode);
            Assert.Equal(order.Quantity, msg.Quantity);
            Assert.Equal(order.CreatedTime, msg.CreatedTime);

            Assert.True((DateTime.Now - msg.CreatedTime).Seconds >= 5);

            //clean up
            channel.QueueDelete(delayInfo.DelayQueue);
            channel.ExchangeDelete(info.Exchange);
        }

        [Fact]
        public void Fanout发送消息()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var channel = _connection.CreateModel();
            mqHub.EnsurePublish(channel, typeof(Order), 0, out QueueInfo info, out _);

            // 消费者1
            var json1 = string.Empty;
            var res = channel.QueueDeclare();
            channel.QueueBind(res.QueueName, info.Exchange, string.Empty);
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                json1 = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(res.QueueName, false, consumer);

            // 消费者2
            var json2 = string.Empty;
            res = channel.QueueDeclare();
            channel.QueueBind(res.QueueName, info.Exchange, string.Empty);
            consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                json2 = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(res.QueueName, false, consumer);

            var order = new Order
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            mqHub.Publish(order);

            Thread.Sleep(1000 * 2);

            var msg1 = JsonConvert.DeserializeObject<Order>(json1);
            var msg2 = JsonConvert.DeserializeObject<Order>(json2);
            Assert.Equal(order.OrderId, msg1.OrderId);
            Assert.Equal(order.Price, msg1.Price);
            Assert.Equal(order.ProductCode, msg1.ProductCode);
            Assert.Equal(order.Quantity, msg1.Quantity);
            Assert.Equal(order.CreatedTime, msg1.CreatedTime);

            Assert.Equal(order.OrderId, msg2.OrderId);
            Assert.Equal(order.Price, msg2.Price);
            Assert.Equal(order.ProductCode, msg2.ProductCode);
            Assert.Equal(order.Quantity, msg2.Quantity);
            Assert.Equal(order.CreatedTime, msg2.CreatedTime);

            // clean up
            channel.ExchangeDelete(info.Exchange);
        }

        [Fact]
        public void Fanout发送消息延时()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var channel = _connection.CreateModel();
            mqHub.EnsurePublish(channel, typeof(Order), 5, out QueueInfo info, out DelayQueueInfo delayInfo);

            // 消费者1
            var json1 = string.Empty;
            var res = channel.QueueDeclare();
            channel.QueueBind(res.QueueName, info.Exchange, string.Empty);
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                json1 = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(res.QueueName, false, consumer);

            // 消费者2
            var json2 = string.Empty;
            res = channel.QueueDeclare();
            channel.QueueBind(res.QueueName, info.Exchange, string.Empty);
            consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                json2 = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(res.QueueName, false, consumer);

            var order = new Order
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            mqHub.Publish(order);

            Thread.Sleep(1000 * 5);

            var msg1 = JsonConvert.DeserializeObject<Order>(json1);
            var msg2 = JsonConvert.DeserializeObject<Order>(json2);
            Assert.Equal(order.OrderId, msg1.OrderId);
            Assert.Equal(order.Price, msg1.Price);
            Assert.Equal(order.ProductCode, msg1.ProductCode);
            Assert.Equal(order.Quantity, msg1.Quantity);
            Assert.Equal(order.CreatedTime, msg1.CreatedTime);
            Assert.True((DateTime.Now - msg1.CreatedTime).Seconds >= 5);

            Assert.Equal(order.OrderId, msg2.OrderId);
            Assert.Equal(order.Price, msg2.Price);
            Assert.Equal(order.ProductCode, msg2.ProductCode);
            Assert.Equal(order.Quantity, msg2.Quantity);
            Assert.Equal(order.CreatedTime, msg2.CreatedTime);
            Assert.True((DateTime.Now - msg2.CreatedTime).Seconds >= 5);

            // clean up
            channel.QueueDelete(delayInfo.DelayQueue);
            channel.ExchangeDelete(info.Exchange);
        }

        [Fact]
        public void 批量发送消息()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var flagOrder = new Order();
            var orders = new List<Order>();
            for (var i = 0; i < 5; i++)
            {
                var order = new Order()
                {
                    OrderId = i.ToString(),
                    Price = 100M,
                    ProductCode = "Computer",
                    Quantity = 10,
                    CreatedTime = DateTime.Now
                };
                orders.Add(order);
            }
            mqHub.Send(orders);

            var json = string.Empty;
            var channel = _connection.CreateModel();
            mqHub.EnsureSendQueue(channel, typeof(Order), out QueueInfo info);
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                flagOrder.Quantity += 1;
                json = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(info.Queue, false, consumer);

            Thread.Sleep(1000 * 5);

            var msg = JsonConvert.DeserializeObject<Order>(json);
            Assert.Equal(orders[4].OrderId, msg.OrderId);
            Assert.Equal(orders[4].Price, msg.Price);
            Assert.Equal(orders[4].ProductCode, msg.ProductCode);
            Assert.Equal(orders[4].Quantity, msg.Quantity);
            Assert.Equal(orders[4].CreatedTime, msg.CreatedTime);
            Assert.Equal(5, flagOrder.Quantity);

            // clean up
            channel.QueueDelete(info.Queue);
        }

        [Fact]
        public void 批量发送消息延时带RouteKey()
        {
            var mqHub = new RabbitMqHub(GetConfig());
            var flagOrder = new Order();
            var orders = new List<Order>();
            for (var i = 0; i < 5; i++)
            {
                var order = new Order()
                {
                    OrderId = i.ToString(),
                    Price = 100M,
                    ProductCode = "Computer",
                    Quantity = 10,
                    CreatedTime = DateTime.Now
                };
                orders.Add(order);
            }
            mqHub.Send(orders, routeKey: "computer", delay: 5);

            var json = string.Empty;
            var channel = _connection.CreateModel();
            mqHub.EnsureRoute(channel, typeof(Order), "computer", 5, out QueueInfo info, out DelayQueueInfo delayInfo);
            var res = channel.QueueDeclare();
            channel.QueueBind(res.QueueName, info.Exchange, "computer");
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                flagOrder.Quantity += 1;
                json = Encoding.UTF8.GetString(ea.Body.ToArray());
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(res.QueueName, false, consumer);

            Thread.Sleep(1000 * 10);

            var msg = JsonConvert.DeserializeObject<Order>(json);
            Assert.Equal(orders[4].OrderId, msg.OrderId);
            Assert.Equal(orders[4].Price, msg.Price);
            Assert.Equal(orders[4].ProductCode, msg.ProductCode);
            Assert.Equal(orders[4].Quantity, msg.Quantity);
            Assert.Equal(orders[4].CreatedTime, msg.CreatedTime);
            Assert.Equal(5, flagOrder.Quantity);

            // clean up
            channel.ExchangeDelete(info.Exchange);
        }

        private IConfigurationRoot GetConfig()
        {
            var builder = new ConfigurationBuilder()
                .AddJsonFile("config/appsettings.json");

            return builder.Build();
        }
    }
}
