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
                message.RecvStatus = RecvStatus.Received;
                return true;
            }

            return false;
        }

        public Task<bool> TrackMessageAsync(Message message)
        {
            var ret = TrackMessage(message);
            return Task.FromResult(ret);
        }

        public void SetStatus(Message message, RecvStatus newStatus, string remark = "")
        {
            _msgList.TryGetValue(message.MsgId, out Message trackedMsg);
            trackedMsg.RecvStatus = newStatus;
            trackedMsg.Remark = remark;
            if (newStatus == RecvStatus.Failed)
            {
                _errorMsgs.Enqueue(trackedMsg);
            }
        }

        public Task SetStatusAsync(Message message, RecvStatus newStatus, string remark = "")
        {
            SetStatus(message, newStatus, remark);
            return Task.CompletedTask;
        }
    }
}
