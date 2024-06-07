using LightMessager;
using System;

namespace ConsumerTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var mqHub = new RabbitMqHub();
            // ============简单的消费者============
            // 最简单的消费者，不需要创建强类型的MessageHandler，只需要绑定一个匿名action即可；
            // 此时库会自动选择消费跟消息类型名称一致的队列（这里是Order）
            mqHub.Consume<Order>(msg =>
            {
                Console.WriteLine(msg);
            });


            // 以下均为强类型的消费端，在这种情况中库会根据消息类型创建一个持久的队列用来消费
            // ============注册一个强类型的消费端============
            //mqHub.Advance.RegisterHandler<Order, OrderHandler>();


            // ============注册感兴趣的routekey进行消费============
            //mqHub.Advance.RegisterHandler<Order, OrderHandler1>("direct_sub1", new string[] { "computer" });
            //System.Threading.Thread.Sleep(1000 * 5);// 睡一下让打印出来的结果清晰一点，不影响测试正确性
            //mqHub.Advance.RegisterHandler<Order, OrderHandler2>("direct_sub2", new string[] { "fruit", "snack" });


            // ============注册感兴趣的topic进行消费============
            //mqHub.Advance.RegisterHandler<Order, OrderHandler1>("topic_sub1", new string[] { "food.#" });
            //System.Threading.Thread.Sleep(1000 * 5);// 睡一下让打印出来的结果清晰一点，不影响测试正确性
            //mqHub.Advance.RegisterHandler<Order, OrderHandler2>("topic_sub2", new string[] { "*.cheap.*" });


            // ============Fanout============
            //mqHub.Advance.RegisterHandler<Order, OrderHandler1>("fanout_sub1");
            //System.Threading.Thread.Sleep(1000 * 5);// 睡一下让打印出来的结果清晰一点，不影响测试正确性
            //mqHub.Advance.RegisterHandler<Order, OrderHandler2>("fanout_sub2");

            Console.Read();
        }
    }
}
