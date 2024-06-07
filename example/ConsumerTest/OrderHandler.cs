using LightMessager.Model;
using System;

namespace ConsumerTest
{
    /// <summary>
    /// 虽然可能很麻烦，但还是强烈建议Handler的逻辑实现成幂等的
    /// </summary>
    public class OrderHandler : BaseMessageHandler<Order>
    {
        public override bool Handle(Order message, bool redelivered)
        {
            // 不需要做任何try...catch处理，库内部已经处理过了
            Console.WriteLine("接收到消息: " + message + "，时间：" + DateTime.Now);
            return true;
        }
    }

    /// <summary>
    /// 虽然可能很麻烦，但还是强烈建议Handler的逻辑实现成幂等的
    /// </summary>
    public class OrderHandler1 : BaseMessageHandler<Order>
    {
        public override bool Handle(Order message, bool redelivered)
        {
            // 不需要做任何try...catch处理，库内部已经处理过了
            Console.WriteLine("我是sub1, 接收到消息: " + message + "，时间：" + DateTime.Now);
            return true;
        }
    }

    /// <summary>
    /// 虽然可能很麻烦，但还是强烈建议Handler的逻辑实现成幂等的
    /// </summary>
    public class OrderHandler2 : BaseMessageHandler<Order>
    {
        public override bool Handle(Order message, bool redelivered)
        {
            // 不需要做任何try...catch处理，库内部已经处理过了
            Console.WriteLine("我是sub2，接收到消息: " + message + "，时间：" + DateTime.Now);
            return true;
        }
    }
}
