using LightMessager;
using System;

namespace ConsumerTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var mq = new RabbitMqHub();
            // ============简单的消费端============
            //mq.RegisterHandler<OrderMessage, OrderHandler>();


            // ============注册感兴趣的routekey进行消费============
            mq.RegisterHandler<Order, OrderHandler1>("direct_sub1", new string[] { "computer" });
            System.Threading.Thread.Sleep(1000 * 5);// 睡一下让打印出来的结果清晰一点，不影响测试正确性
            mq.RegisterHandler<Order, OrderHandler2>("direct_sub2", new string[] { "fruit", "snack" });


            // ============注册感兴趣的topic进行消费============
            //mq.RegisterHandler<Order, OrderHandler1>("topic_sub1", new string[] { "food.#" });
            //System.Threading.Thread.Sleep(1000 * 5);// 睡一下让打印出来的结果清晰一点，不影响测试正确性
            //mq.RegisterHandler<Order, OrderHandler2>("topic_sub2", new string[] { "*.cheap.*" });


            // ============Fanout============
            //mq.RegisterHandler<Order, OrderHandler1>("fanout_sub1");
            //System.Threading.Thread.Sleep(1000 * 5);// 睡一下让打印出来的结果清晰一点，不影响测试正确性
            //mq.RegisterHandler<Order, OrderHandler2>("fanout_sub2");
            Console.Read();
        }
    }
}
