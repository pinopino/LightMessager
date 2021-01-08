using LightMessager.Model;
using System.Collections.Generic;

namespace LightMessager.Track
{
    public class InMemoryTracker : BaseMessageTracker
    {
        private object _lockObj;
        private List<BaseMessage> _list;

        public InMemoryTracker()
        {
            _lockObj = new object();
            _list = new List<BaseMessage>();
        }

        public override bool AddMessage(BaseMessage message)
        {
            lock (_lockObj)
                _list.Add(message);

            return true;
        }

        public override BaseMessage GetMessage(string messageId)
        {
            lock (_lockObj)
            {
                foreach (var item in _list)
                {
                    if (item.MsgId == messageId)
                        return item;
                }
            }
            return null;
        }

        public override void SetStatus(ulong deliveryTag, MessageState newStatus, MessageState oldStatus = MessageState.Created, string remark = "")
        {
            if (UnRegisterMap(deliveryTag, out string msgId))
            {
                lock (_lockObj)
                {
                    foreach (var msg in _list)
                    {
                        if (msg.MsgId == msgId && (oldStatus == MessageState.Created || msg.State == oldStatus))
                        {
                            msg.State = newStatus;
                            break;
                        }
                    }
                    ClearList();
                }
            }
        }

        public override void SetMultipleStatus(ulong deliveryTag, MessageState newStatus, MessageState oldStatus = MessageState.Created)
        {
            var list = UnRegisterMap(deliveryTag);
            if (list != null)
            {
                lock (_lockObj)
                {
                    foreach (var item in _list)
                    {
                        if (list.Contains(item.MsgId) && (oldStatus == MessageState.Created || item.State == oldStatus))
                            item.State = newStatus;
                    }
                    ClearList();
                }
            }
        }

        public override void SetStatus(string messageId, MessageState newStatus, MessageState oldStatus = MessageState.Created, string remark = "")
        {
            lock (_lockObj)
            {
                foreach (var msg in _list)
                {
                    if (msg.MsgId == messageId && (oldStatus == MessageState.Created || msg.State == oldStatus))
                    {
                        msg.State = newStatus;
                        break;
                    }
                }
                ClearList();
            }
        }

        // 清理逻辑，防止_list过大
        private void ClearList()
        {
            // 清理逻辑，防止_list过大
            if (_list.Count > 1000)
            {
                List<BaseMessage> newList = null;
                foreach (var item in _list)
                {
                    if (item.State != MessageState.Confirmed && item.State != MessageState.Consumed)
                    {
                        if (newList == null)
                            newList = new List<BaseMessage>();
                        newList.Add(item);
                    }
                }
            }
        }
    }
}
