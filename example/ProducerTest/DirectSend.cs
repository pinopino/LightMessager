using LightMessager;
using System;

namespace ProducerTest
{
    public class DirectSend
    {
        RabbitMqHub _mqHub;
        public DirectSend(RabbitMqHub mqHub)
        {
            _mqHub = mqHub;
        }

        public void Run1()
        {
            var order = new OrderMessage
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            _mqHub.Send(order);
        }

        // round-robin dispatching（默认）
        public void Run2()
        {
            for (var i = 0; i < 10; i++)
            {
                var order = new OrderMessage
                {
                    OrderId = $"order_{i}",
                    Price = (i + 1) * 100M,
                    ProductCode = "Computer",
                    Quantity = (i + 1) * 10,
                    CreatedTime = DateTime.Now
                };
                _mqHub.Send(order);
            }
        }

        public void Run3(int delay)
        {
            var order = new OrderMessage
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            Console.WriteLine("发送一条消息，时间：" + DateTime.Now);
            _mqHub.Send(order, delaySend: delay);
        }
    }
}
