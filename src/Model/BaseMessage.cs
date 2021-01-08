using Newtonsoft.Json;
using System;

namespace LightMessager.Model
{
    public abstract class BaseMessage
    {
        public abstract string MsgId { get; }

        [JsonIgnore]
        public string Content { set; get; }

        /// <summary>
        /// 0 created、1 error、2 error_unroutable、4 error_noexchangefound、8 confirmed、16 consumed
        /// </summary>
        public MessageState State { set; get; }

        /// <summary>
        /// publisher -> broker
        /// </summary>
        public int Republish { set; get; }

        /// <summary>
        /// broker -> consumer
        /// </summary>
        public int Requeue { set; get; }

        [JsonIgnore]
        public string Remark { set; get; }

        public DateTime CreatedTime { set; get; }

        public DateTime? ModifyTime { set; get; }

        [JsonIgnore]
        internal ulong DeliveryTag { set; get; }

        [JsonIgnore]
        internal bool NeedRequeue { set; get; }

        [JsonIgnore]
        internal string Pattern { set; get; }
    }
}
