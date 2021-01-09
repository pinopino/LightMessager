using LightMessager;
using System;

namespace ProducerTest
{
    public class SendWithRoutekey
    {
        RabbitMqHub _mqHub;
        public SendWithRoutekey(RabbitMqHub mqHub)
        {
            _mqHub = mqHub;
        }

        public void Run1()
        {
            for (var i = 0; i < 5; i++)
            {
                var order1 = new OrderMessage
                {
                    OrderId = $"电脑订单{i}",
                    Price = 100M,
                    ProductCode = "Computer",
                    Quantity = 10,
                    CreatedTime = DateTime.Now
                };
                _mqHub.Send(order1, "computer");
            }

            for (var i = 0; i < 3; i++)
            {
                var order2 = new OrderMessage
                {
                    OrderId = $"水果订单{i}",
                    Price = 200M,
                    ProductCode = "Fruit",
                    Quantity = 20,
                    CreatedTime = DateTime.Now
                };
                _mqHub.Send(order2, "fruit");
            }

            for (var i = 0; i < 3; i++)
            {
                var order3 = new OrderMessage
                {
                    OrderId = $"零食订单{i}",
                    Price = 300M,
                    ProductCode = "Snack",
                    Quantity = 30,
                    CreatedTime = DateTime.Now
                };
                _mqHub.Send(order3, "snack");
            }
        }

        public void Run2()
        {
            var order1 = new OrderMessage
            {
                OrderId = "电脑订单",
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            _mqHub.Send(order1, "tools.cheap.computer");

            var order2 = new OrderMessage
            {
                OrderId = "水果订单",
                Price = 200M,
                ProductCode = "Fruit",
                Quantity = 20,
                CreatedTime = DateTime.Now
            };
            _mqHub.Send(order2, "food.cheap.fruit");

            var order3 = new OrderMessage
            {
                OrderId = "零食订单",
                Price = 300M,
                ProductCode = "Snack",
                Quantity = 30,
                CreatedTime = DateTime.Now
            };
            _mqHub.Send(order3, "food.snack");
        }

        public void Run3(int delay)
        {
            for (var i = 0; i < 3; i++)
            {
                var order2 = new OrderMessage
                {
                    OrderId = $"水果订单{i}",
                    Price = 200M,
                    ProductCode = "Fruit",
                    Quantity = 20,
                    CreatedTime = DateTime.Now
                };
                Console.WriteLine("发送一条消息，时间：" + DateTime.Now);
                _mqHub.Send(order2, "fruit", delay);
            }
        }
    }
}
