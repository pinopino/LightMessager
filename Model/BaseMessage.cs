using Newtonsoft.Json;
using System;

namespace LightMessager.Model
{
    public class BaseMessage
    {
        public string MsgId { set; get; }

        [JsonIgnore]
        public string Content { set; get; }

        /// <summary>
        /// 1 created、2 persistent、3 consumed、4 error、5 error_unroutable
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
