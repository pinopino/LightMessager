using LightMessager.Model;
using System.Collections.Concurrent;
using System.Text;

namespace WebApplication1
{
    public class MessageTracker
    {
        private ConcurrentDictionary<int, ConcurrentDictionary<ulong, Message>> _statesByDeliveryTag;  // <ChannelNumber, <DeliveryTag, Msg>>
        private ConcurrentQueue<Message> _errorMsgs;
        public MessageTracker()
        {
            _statesByDeliveryTag = new ConcurrentDictionary<int, ConcurrentDictionary<ulong, Message>>();
            _errorMsgs = new ConcurrentQueue<Message>();
        }

        public void OnMessageSending(object? sender, MessageSendEventArgs e)
        {
            var dict = _statesByDeliveryTag.GetOrAdd(e.ChannelNumber, p => new ConcurrentDictionary<ulong, Message>());
            dict.TryAdd(e.DeliveryTag, e.Message);
        }

        public void OnMessageSendOK(object? sender, MessageSendEventArgs e)
        {
            var dict = _statesByDeliveryTag.GetOrAdd(e.ChannelNumber, p => new ConcurrentDictionary<ulong, Message>());
            if (e.Multiple)
            {
                foreach (var item in dict)
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
                if (dict.TryGetValue(e.DeliveryTag, out Message msg))
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
                var dict = _statesByDeliveryTag.GetOrAdd(e.ChannelNumber, p => new ConcurrentDictionary<ulong, Message>());
                if (e.Multiple)
                {
                    foreach (var item in dict)
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
                    if (dict.TryGetValue(e.DeliveryTag, out Message msg))
                    {
                        msg.SendStatus = e.SendStatus;
                        msg.Remark = e.Remark;
                    }
                }
            }
            else
            {
                _errorMsgs.Enqueue(e.Message);
            }
        }

        public string Summarize()
        {
            var total = 0;
            var success = 0;
            var failed1 = 0;
            foreach (var channel in _statesByDeliveryTag)
            {
                var dict = channel.Value;
                total += dict.Count;
                foreach (var item in dict)
                {
                    if (item.Value.SendStatus == SendStatus.Confirmed)
                        success += 1;
                    else
                        failed1 += 1;
                }
            }
            var failed2 = _errorMsgs.Count;
            
            var sb = new StringBuilder();
            sb.Append($"总计发送消息：{total}条。</br>");
            sb.Append($"成功共计：{success}条；</br>");
            sb.Append($"失败共计：{failed1 + failed2}条，其中nack：{failed1}条，异常情况：{failed2}条；</br>");
            sb.Append($"失败详情：</br>");
            while (!_errorMsgs.IsEmpty)
            {
                if (_errorMsgs.TryDequeue(out Message msg))
                    sb.Append($"消息[{msg.MsgId}]发送失败，状态：{msg.SendStatus}，原因：{msg.Remark}；</br>");
            }
            return sb.ToString();
        }
    }
}
