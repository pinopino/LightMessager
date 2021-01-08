using LightMessager.Model;

namespace ProducerTest
{
    public class OrderMessage : BaseMessage
    {
        public string OrderId;
        public decimal Price;
        public string ProductCode;
        public int Quantity;

        // 设计上MsgId就是用来唯一标识一条消息的；通常我们通过消息本身另外的某个字段计算出来
        public override string MsgId => this.OrderId;

        public override string ToString()
        {
            return $"订单[{OrderId}]，Type[{ProductCode}]";
        }
    }
}
