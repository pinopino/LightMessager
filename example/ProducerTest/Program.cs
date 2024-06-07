using LightMessager;
using System;

namespace ProducerTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var mq = new RabbitMqHub();

            #region baisc
            // 场景1：
            // 直发单条消息
            var simple = new SimpleSend(mq);
            simple.Run1();

            // 场景2：
            // 直发多条消息（同时打开多个消费者），每个消费者消费不同的消息
            //simple.Run2();

            // 场景3：
            // 延迟发送，延迟时间尽量不要太小了以免遇到未知边界情况
            //simple.Run3(10);

            // 场景4：
            // 多条消息借由routekey分发到不同的消费端（消费端需要自己注册感兴趣的routekey）
            // 注意分发时的key要跟消费端注册的key完全一致才能收到消息
            //var route = new SendWithRoutekey(mq);
            //route.Run1();

            // 场景5：
            // 多条消息借由routekey分发到不同的消费端（消费端需要自己注册感兴趣的routekey）
            // routekey满足rabbitmq topic发送规则，类似xxx.xxx.xxx
            //route.Run2();

            // 场景6：
            // 多条消息借由routekey分发到不同的消费端，延迟发送
            //route.Run3(10);

            // 场景7：
            // 多条消息fanout方式的发送，所有消费者消费相同的消息
            //var fanout = new Fanout(mq);
            //fanout.Run();
            #endregion

            Console.Read();
        }
    }
}
