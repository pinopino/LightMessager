using LightMessager;
using System;

namespace ProducerTest
{
    public class Fanout
    {
        RabbitMqHub _mqHub;
        public Fanout(RabbitMqHub mqHub)
        {
            _mqHub = mqHub;
        }

        public void Run()
        {
            var order = new OrderMessage
            {
                OrderId = Guid.NewGuid().ToString(),
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10,
                CreatedTime = DateTime.Now
            };
            _mqHub.Publish(order);
        }
    }
}
