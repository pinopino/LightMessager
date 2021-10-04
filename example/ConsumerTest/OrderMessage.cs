using LightMessager.Model;

namespace ConsumerTest
{
    // 消息类型按理来说应该是提取到一个公共的程序集中方便生产方和消费方共用；
    // 这里图简便并没有这样做，当然这也展示另外一种可能即消息并不是按照类型的强名称来区分的
    //（无关程序集，只是简单的类型名称区分即可）
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
