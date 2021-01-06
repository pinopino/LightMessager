using LightMessager.Model;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    public abstract class BaseMessageTracker
    {
        // 一个用于同步，一个用于异步
        protected ConcurrentDictionary<ulong, string> _unconfirm; // <DeliveryTag, MsgId>
        protected ConcurrentDictionary<ulong, TaskCompletionSource<int>> _awaitingConfirms;

        public void Init()
        {
            _unconfirm = new ConcurrentDictionary<ulong, string>();
            _awaitingConfirms = new ConcurrentDictionary<ulong, TaskCompletionSource<int>>();
        }

        public abstract bool IsExist(string messageId);

        public abstract bool AddMessage(BaseMessage message);

        public abstract BaseMessage GetMessage(string messageId);

        public abstract void SetStatus(ulong deliveryTag, MessageState newStatus, MessageState oldStatus = MessageState.None, string remark = "");

        public abstract void SetMultipleStatus(ulong deliveryTag, MessageState newStatus, MessageState oldStatus = MessageState.None);

        public abstract void SetStatus(string messageId, MessageState newStatus, MessageState oldStatus = MessageState.None, string remark = "");

        public virtual void TrackMessage(ulong deliveryTag, string messageId)
        {
            SetDeliveryTag(deliveryTag, messageId);
        }

        internal void SetDeliveryTag(ulong deliveryTag, string messageId)
        {
            _unconfirm.TryAdd(deliveryTag, messageId);
        }

        internal Task SetDeliveryTagForAsync(ulong deliveryTag, string messageId)
        {
            var tcs = new TaskCompletionSource<int>(messageId);
            _awaitingConfirms.TryAdd(deliveryTag, tcs);
            return tcs.Task;
        }
    }
}
