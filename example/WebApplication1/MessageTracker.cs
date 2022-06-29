using LightMessager.Model;
using System.Collections.Concurrent;

namespace WebApplication1
{
    public class MessageTracker
    {
        private ConcurrentDictionary<ulong, Message> _unconfirm; // <DeliveryTag, Msg>
        private ConcurrentQueue<Message> _errorMsgs;

        public void OnMessageSending(object? sender, MessageSendEventArgs e)
        {
            _unconfirm.TryAdd(e.DeliveryTag, e.Message);
        }

        public void OnMessageSendOK(object? sender, MessageSendEventArgs e)
        {
            if (e.Multiple)
            {
                foreach (var item in _unconfirm)
                {
                    if (item.Key > 0 && item.Key <= e.DeliveryTag)
                    {
                        item.Value.SendStatus = e.SendStatus;
                        item.Value.Remark = e.Remark;
                    }
                }
            }
            else
            {
                if (_unconfirm.TryGetValue(e.DeliveryTag, out Message msg))
                {
                    msg.SendStatus = e.SendStatus;
                    msg.Remark = e.Remark;
                }
            }
        }

        public void OnMessageSendFailed(object? sender, MessageSendEventArgs e)
        {
            if (e.SendStatus == SendStatus.Nacked)
            {
                if (e.Multiple)
                {

                }
                else
                {

                }
            }
            else
            {
                foreach (var msg in _unconfirm.Values)
                {

                }
            }
        }
    }
}
