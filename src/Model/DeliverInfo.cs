namespace LightMessager.Model
{
    public class DeliverInfo
    {
        public byte[] Body { set; get; }
        public ulong DeliveryTag { set; get; }
        public bool Redelivered { set; get; }
    }
}
