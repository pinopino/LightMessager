using LightMessager.Model;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    /// <summary>
    /// for debug only
    /// </summary>
    internal sealed class InMemorySendTracker
    {
        private readonly int _spinCount;
        private volatile int _reseting;
        private ConcurrentDictionary<ulong, Message> _unconfirm; // <DeliveryTag, Msg>
        private ConcurrentQueue<Message> _errorMsgs;

        public InMemorySendTracker()
        {
            _spinCount = 200;
            _unconfirm = new ConcurrentDictionary<ulong, Message>();
            _errorMsgs = new ConcurrentQueue<Message>();
        }

        public Task TrackMessageAsync(ulong deliveryTag, Message message)
        {
            while (_reseting > 0)
                Thread.SpinWait(_spinCount);

            _unconfirm.TryAdd(deliveryTag, message);
            message.DeliveryTag = deliveryTag;

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

        public Task ResetAsync(string remark = "")
        {
            Interlocked.Increment(ref _reseting);
            var old = _unconfirm;
            _unconfirm = new ConcurrentDictionary<ulong, Message>();
            Interlocked.Decrement(ref _reseting);

            foreach (var item in old)
            {
                if (item.Value.SendStatus != SendStatus.Confirmed)
                {
                    item.Value.Remark = remark;
                    _errorMsgs.Enqueue(item.Value);
                }
            }

            return Task.CompletedTask;
        }
    }
}
