using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace LightMessager.Model
{
    public interface IIdentifiedMessage
    {
        string MsgId { get; }
    }

    public abstract class Message
    {
        public Dictionary<string, object> Headers { protected set; get; }

        internal ulong DeliveryTag { set; get; }

        public SendStatus SendStatus { set; get; }

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

        public Message(TBody body, Dictionary<string, object> headers)
        {
            Body = body;
            Headers = headers;
        }
    }
}
