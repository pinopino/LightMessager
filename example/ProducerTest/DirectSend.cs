using LightMessager;
using System;

namespace ProducerTest
{
    public class SimpleSend
    {
        RabbitMqHub _mqHub;
        public SimpleSend(RabbitMqHub mqHub)
        {
            _mqHub = mqHub;
        }

        // 简单发送
        // 库会根据消息类型（这里是Order）自动创建一个队列，并发送消息（下同）
        public void Run1()
        {
            var order = new Order
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10
            };
            _mqHub.Send(order);
        }

        // 发送多条消息
        public void Run2()
        {
            for (var i = 0; i < 10; i++)
            {
                var order = new Order
                {
                    OrderId = $"order_{i}",
                    Price = (i + 1) * 100M,
                    ProductCode = "Computer",
                    Quantity = (i + 1) * 10
                };
                _mqHub.Send(order);
            }
        }

        // 延迟发送一条消息
        public void Run3(int delay)
        {
            var order = new Order
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            _mqHub.Send(order, delay: delay);
        }
    }
}
