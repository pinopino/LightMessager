using LightMessager.Common;
using Newtonsoft.Json;
using System;

namespace LightMessager.Model
{
    public interface IIdentifiedMessage
    {
        string MsgId { get; }
    }

    public abstract class Message
    {
        private Lazy<string> _id;
        public Message()
        {
            _id = new Lazy<string>(() => $"Auto_{ObjectId.GenerateNewId()}");
        }

        public virtual string MsgId => _id.Value;

        internal ulong DeliveryTag { set; get; }

        // 说明：消息的每次处理我们都需要判断是否需要重新入队列，
        // 因此它不应该随着消息的序列化而序列化
        [JsonIgnore]
        internal bool NeedRequeue { set; get; }

        public SendStatus SendStatus { set; get; }

        public RecvStatus RecvStatus { set; get; }

        internal abstract object GetBody();

        [JsonIgnore]
        public string Remark { set; get; }

        public DateTime CreatedTime { set; get; }
    }

    public sealed class Message<TBody> : Message
    {
        public TBody Body { set; get; }

        public Message()
        { }

        public Message(TBody body)
        {
            Body = body;
        }

        public override string MsgId
        {
            get
            {
                if (typeof(IIdentifiedMessage).IsAssignableFrom(typeof(TBody)))
                {
                    return (Body as IIdentifiedMessage).MsgId;
                }
                return base.MsgId;
            }
        }

        internal override object GetBody()
        {
            return Body;
        }
    }
}
