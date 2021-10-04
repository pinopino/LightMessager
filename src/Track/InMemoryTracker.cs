using LightMessager.Model;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    public sealed class InMemoryTracker : IMessageSendTracker
    {
        private readonly int _spinCount;
        private volatile int _reseting;
        private ConcurrentDictionary<ulong, Message> _unconfirm; // <DeliveryTag, Msg>
        private ConcurrentQueue<Message> _errorMsg;

        public InMemoryTracker()
        {
            _spinCount = 200;
            _unconfirm = new ConcurrentDictionary<ulong, Message>();
            _errorMsg = new ConcurrentQueue<Message>();
        }

        public void TrackMessage(ulong deliveryTag, Message message)
        {
            // 说明：
            // 
            // 链接：
            // https://stackoverflow.com/questions/11099852/lock-vs-boolean
            // https://stackoverflow.com/questions/154551/volatile-vs-interlocked-vs-lock
            // https://docs.microsoft.com/en-us/dotnet/api/system.threading.interlocked.compareexchange
            while (_reseting > 0)
                Thread.SpinWait(_spinCount);

            _unconfirm.TryAdd(deliveryTag, message);
            message.DeliveryTag = deliveryTag;
        }

        public ValueTask TrackMessageAsync(ulong deliveryTag, Message message)
        {
            TrackMessage(deliveryTag, message);
            return new ValueTask(Task.CompletedTask);
        }

        public void SetStatus(ulong deliveryTag, SendStatus newStatus, string remark = "")
        {
            if (_unconfirm.TryGetValue(deliveryTag, out Message msg))
            {
                msg.SendStatus = newStatus;
                msg.Remark = remark;
            }
        }

        public ValueTask SetStatusAsync(ulong deliveryTag, SendStatus newStatus, string remark = "")
        {
            SetStatus(deliveryTag, newStatus, remark);
            return new ValueTask(Task.CompletedTask);
        }

        public void SetMultipleStatus(ulong deliveryTag, SendStatus newStatus)
        {
            foreach (var item in _unconfirm)
            {
                if (item.Key > 0 && item.Key <= deliveryTag)
                {
                    item.Value.SendStatus = newStatus;
                }
            }
        }

        public ValueTask SetMultipleStatusAsync(ulong deliveryTag, SendStatus newStatus)
        {
            SetMultipleStatus(deliveryTag, newStatus);
            return new ValueTask(Task.CompletedTask);
        }

        public void SetStatus(Message message, SendStatus newStatus, string remark = "")
        {
            while (_reseting > 0)
                Thread.SpinWait(_spinCount);
            _unconfirm.TryRemove(message.DeliveryTag, out _);

            message.SendStatus = newStatus;
            message.Remark = remark;
            _errorMsg.Enqueue(message);
        }

        public ValueTask SetStatusAsync(Message message, SendStatus newStatus, string remark = "")
        {
            SetStatus(message, newStatus, remark);
            return new ValueTask(Task.CompletedTask);
        }

        public void Reset(string remark = "")
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
                    _errorMsg.Enqueue(item.Value);
                }
            }
        }

        public ValueTask ResetAsync()
        {
            Reset();
            return new ValueTask(Task.CompletedTask);
        }
    }
}
