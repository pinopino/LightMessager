using LightMessager.Model;
using System.Collections.Concurrent;

namespace WebApplication1.Track
{
    /// <summary>
    /// for debug only
    /// </summary>
    public sealed class InMemorySendTracker
    {
        private ConcurrentDictionary<ulong, Message> _unconfirm; // <DeliveryTag, Msg>
        private ConcurrentQueue<Message> _errorMsgs;

        public InMemorySendTracker()
        {
            _unconfirm = new ConcurrentDictionary<ulong, Message>();
            _errorMsgs = new ConcurrentQueue<Message>();
        }

        public Task TrackMessageAsync(ulong deliveryTag, Message message)
        {
            _unconfirm.TryAdd(deliveryTag, message);

            return Task.CompletedTask;
        }

        public Task SetStatusAsync(ulong deliveryTag, SendStatus newStatus, string remark = "")
        {
            if (_unconfirm.TryGetValue(deliveryTag, out Message msg))
            {
                msg.SendStatus = newStatus;
                msg.Remark = remark;
            }

            return Task.CompletedTask;
        }

        public Task SetMultipleStatusAsync(ulong deliveryTag, SendStatus newStatus, string remark = "")
        {
            foreach (var item in _unconfirm)
            {
                if (item.Key > 0 && item.Key <= deliveryTag)
                {
                    item.Value.SendStatus = newStatus;
                }
            }

            return Task.CompletedTask;
        }

        public Task SetErrorAsync(Message message, SendStatus newStatus, string remark = "")
        {
            throw new System.NotImplementedException();
        }
    }
}
