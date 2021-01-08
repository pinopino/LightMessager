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
            // 场景1：1v1 direct send
            //var direct = new DirectSend(mq);
            //direct.Run1();

            // 场景2：1vN direct send
            //direct.Run2();

            // 场景3：一堆消息借由routekey分发到不同的消费端（消费端需要自己注册感兴趣的routekey）
            //var route = new SendWithRoutekey(mq);
            //route.Run();

            // 场景4：fanout send
            var fanout = new Fanout(mq);
            fanout.Run();
            #endregion
        }
    }
}
