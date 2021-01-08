using LightMessager.Model;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    public abstract class BaseMessageTracker
    {
        // 一个用于同步，一个用于异步
        protected ConcurrentDictionary<ulong, string> _unconfirm; // <DeliveryTag, MsgId>
        protected ConcurrentDictionary<ulong, TaskCompletionSource<int>> _awaitingCheckpoint;

        public BaseMessageTracker()
        {
            Reset();
        }

        public void Reset()
        {
            _unconfirm = new ConcurrentDictionary<ulong, string>();
            _awaitingCheckpoint = new ConcurrentDictionary<ulong, TaskCompletionSource<int>>();
        }

        public abstract bool AddMessage(BaseMessage message);

        public abstract BaseMessage GetMessage(string messageId);

        public abstract void SetStatus(ulong deliveryTag, MessageState newStatus, MessageState oldStatus = MessageState.Created, string remark = "");

        public abstract void SetMultipleStatus(ulong deliveryTag, MessageState newStatus, MessageState oldStatus = MessageState.Created);

        public abstract void SetStatus(string messageId, MessageState newStatus, MessageState oldStatus = MessageState.Created, string remark = "");

        public virtual void TrackMessage(ulong deliveryTag, string messageId)
        {
            RegisterMap(deliveryTag, messageId);
        }

        internal void RegisterMap(ulong deliveryTag, string messageId)
        {
            _unconfirm.TryAdd(deliveryTag, messageId);
        }

        internal bool UnRegisterMap(ulong deliveryTag, out string messageId)
        {
            return _unconfirm.TryRemove(deliveryTag, out messageId);
        }

        internal List<string> UnRegisterMap(ulong deliveryTagUpperBound)
        {
            List<string> ret = null;
            var confirmed = _unconfirm.Keys.Where(p => p <= deliveryTagUpperBound).ToList();
            foreach (var item in confirmed)
            {
                _unconfirm.TryRemove(item, out string id);
                if (ret == null)
                    ret = new List<string>();
                ret.Add(id);
            }

            return ret;
        }

        internal Task RegisterMapForAsync(ulong deliveryTag, string messageId)
        {
            var tcs = new TaskCompletionSource<int>(messageId);
            _awaitingCheckpoint.TryAdd(deliveryTag, tcs);
            return tcs.Task;
        }
    }
}
