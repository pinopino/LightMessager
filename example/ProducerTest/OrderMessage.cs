using LightMessager.Model;

namespace ProducerTest
{
    public class Order : IIdentifiedMessage
    {
        public string OrderId;
        public decimal Price;
        public string ProductCode;
        public int Quantity;

        // 设计上MsgId就是用来唯一标识一条消息的；通常可以使用消息本身的某些字段计算出来
        public string MsgId => this.OrderId;

        public override string ToString()
        {
            return $"订单[{OrderId}]，Type[{ProductCode}]";
        }
    }
}
