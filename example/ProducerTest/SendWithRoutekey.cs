using LightMessager;

namespace ProducerTest
{
    public class SendWithRoutekey
    {
        RabbitMqHub _mqHub;
        public SendWithRoutekey(RabbitMqHub mqHub)
        {
            _mqHub = mqHub;
        }

        // 发送带有routkey的消息
        // 库在此种模式下会根据routkey的特性自动选择为你创建一个exchange（type为direct、route、fanout都有可能）
        // 不会创建queue，你需要自己手动调用Consume、RegisterHandler等消费端方法来进行消费
        public void Run1()
        {
            for (var i = 0; i < 5; i++)
            {
                var order1 = new Order
                {
                    OrderId = $"电脑订单{i}",
                    Price = 100M,
                    ProductCode = "Computer",
                    Quantity = 10
                };
                _mqHub.Send(order1, "computer");
            }

            for (var i = 0; i < 3; i++)
            {
                var order2 = new Order
                {
                    OrderId = $"水果订单{i}",
                    Price = 200M,
                    ProductCode = "Fruit",
                    Quantity = 20
                };
                _mqHub.Send(order2, "fruit");
            }

            for (var i = 0; i < 3; i++)
            {
                var order3 = new Order
                {
                    OrderId = $"零食订单{i}",
                    Price = 300M,
                    ProductCode = "Snack",
                    Quantity = 30
                };
                _mqHub.Send(order3, "snack");
            }
        }

        public void Run2()
        {
            var order1 = new Order
            {
                OrderId = "电脑订单",
                Price = 100M,
                ProductCode = "Computer",
                Quantity = 10
            };
            _mqHub.Send(order1, "tools.cheap.computer");

            var order2 = new Order
            {
                OrderId = "水果订单",
                Price = 200M,
                ProductCode = "Fruit",
                Quantity = 20
            };
            _mqHub.Send(order2, "food.cheap.fruit");

            var order3 = new Order
            {
                OrderId = "零食订单",
                Price = 300M,
                ProductCode = "Snack",
                Quantity = 30
            };
            _mqHub.Send(order3, "food.snack");
        }
    }
}
