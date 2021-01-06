using LightMessager.Model;
using System;
using System.Collections.Generic;
using System.Linq;

namespace LightMessager.Track
{
    public class InMemoryTracker : BaseMessageTracker
    {
        // To keep all messages to be sent
        private List<BaseMessage> _list;

        public InMemoryTracker()
        {
            _list = new List<BaseMessage>();
        }

        public override bool IsExist(string messageId)
        {
            return false;
        }

        public override bool AddMessage(BaseMessage message)
        {
            throw new NotImplementedException();
        }

        public override BaseMessage GetMessage(string messageId)
        {
            throw new NotImplementedException();
        }

        public override void SetStatus(ulong deliveryTag, MessageState newStatus, MessageState oldStatus = MessageState.None, string remark = "")
        {
            if (_unconfirm.TryRemove(deliveryTag, out string msgId))
            {
                foreach (var msg in _list)
                {
                    if (msg.MsgId == msgId && (oldStatus == MessageState.None || msg.State == oldStatus))
                    {
                        msg.State = newStatus;
                        break;
                    }
                }
            }
        }

        public override void SetMultipleStatus(ulong deliveryTag, MessageState newStatus, MessageState oldStatus = MessageState.None)
        {
            List<string> list = null;
            var confirmed = _unconfirm.Keys.Where(p => p <= deliveryTag).ToList();
            foreach (var item in confirmed)
            {
                _unconfirm.TryRemove(item, out string id);
                if (list == null)
                    list = new List<string>();
                list.Add(id);
            }

            if (list != null)
            {
                foreach (var item in _list)
                {
                    if (list.Contains(item.MsgId) && (oldStatus == MessageState.None || item.State == oldStatus))
                        item.State = newStatus;
                }
            }
        }

        public override void SetStatus(string messageId, MessageState newStatus, MessageState oldStatus = MessageState.None, string remark = "")
        {
            foreach (var msg in _list)
            {
                if (msg.MsgId == messageId && (oldStatus == MessageState.None || msg.State == oldStatus))
                {
                    msg.State = newStatus;
                    break;
                }
            }
        }
    }
}
