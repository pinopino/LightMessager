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
            var direct = new DirectSend(mq);
            direct.Run1();

            // 场景2：
            // 直发多条消息（同时打开多个消费者），每个消费者消费不同的消息
            direct.Run2();

            // 场景3：
            // 多条消息借由routekey分发到不同的消费端（消费端需要自己注册感兴趣的routekey）
            // 注意分发时的key要跟消费端注册的key完全一致才能收到消息
            var route = new SendWithRoutekey(mq);
            route.Run();

            // 场景4：
            // 多条消息fanout方式的发送，所有消费者消费相同的消息
            var fanout = new Fanout(mq);
            fanout.Run();
            #endregion
        }
    }
}
