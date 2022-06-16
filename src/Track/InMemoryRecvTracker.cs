using LightMessager.Model;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    /// <summary>
    /// for debug only
    /// </summary>
    internal sealed class InMemoryRecvTracker
    {
        private ConcurrentDictionary<string, Message> _msgList; // <MsgId, Msg>
        private ConcurrentQueue<Message> _errorMsgs;

        public InMemoryRecvTracker()
        {
            _msgList = new ConcurrentDictionary<string, Message>();
            _errorMsgs = new ConcurrentQueue<Message>();
        }

        public bool TrackMessage(Message message)
        {
            if (_msgList.TryAdd(message.MsgId, message))
            {
                message.ConsumeStatus = ConsumeStatus.Received;
                return true;
            }

            return false;
        }

        public Task<bool> TrackMessageAsync(Message message)
        {
            var ret = TrackMessage(message);
            return Task.FromResult(ret);
        }

        public void SetStatus(Message message, ConsumeStatus newStatus, string remark = "")
        {
            _msgList.TryGetValue(message.MsgId, out Message trackedMsg);
            trackedMsg.ConsumeStatus = newStatus;
            trackedMsg.Remark = remark;
            if (newStatus == ConsumeStatus.Failed)
            {
                _errorMsgs.Enqueue(trackedMsg);
            }
        }

        public Task SetStatusAsync(Message message, ConsumeStatus newStatus, string remark = "")
        {
            SetStatus(message, newStatus, remark);
            return Task.CompletedTask;
        }
    }
}
