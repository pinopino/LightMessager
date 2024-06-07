using System;

namespace WebApplication1
{
    public class Order
    {
        public string OrderId { set; get; }
        public decimal Price { set; get; }
        public string ProductCode { set; get; }
        public int Quantity { set; get; }
        public DateTime CreatedTime { set; get; }

        public override string ToString()
        {
            return $"订单[{OrderId}]，Type[{ProductCode}]";
        }
    }
}
