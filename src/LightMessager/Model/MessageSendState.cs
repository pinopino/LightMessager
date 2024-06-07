using Yitter.IdGenerator;

namespace LightMessager.Model
{
    public class MessageSendState
    {
        static MessageSendState()
        {
            // 创建 IdGeneratorOptions 对象，可在构造函数中输入 WorkerId：
            var options = new IdGeneratorOptions();
            // 保存参数（务必调用，否则参数设置不生效）：
            YitIdHelper.SetIdGenerator(options);
        }

        public MessageSendState()
        { }

        internal MessageSendState(ulong deliveryTag, object payload)
        {
            SequenceNumber = deliveryTag;
            MessageId = YitIdHelper.NextId();
            MessagePayload = payload;
        }

        public long MessageId { get; set; }
        public object MessagePayload { get; set; }
        public SendStatus Status { get; set; }
        public bool Acked { get; set; }
        public bool Multiple { get; set; }
        public long ChannelId { get; set; }
        public ulong SequenceNumber { get; set; }
        public string Remark { get; set; }
    }
}
