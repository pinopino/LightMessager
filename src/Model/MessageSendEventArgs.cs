namespace LightMessager.Model
{
    public class MessageSendEventArgs
    {
        public int ChannelNumber { set; get; }
        public ulong DeliveryTag { get; set; }
        public bool Multiple { get; set; }
        public SendStatus SendStatus { set; get; }
        public ushort ReplyCode { set; get; }
        public string ReplyText { set; get; }
        public Message Message { set; get; }
        public string Remark { set; get; }
    }
}
