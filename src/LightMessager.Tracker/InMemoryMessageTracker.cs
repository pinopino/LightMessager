using LightMessager.Model;
using System.Collections.Concurrent;

namespace LightMessager.Tracker
{
    public class InMemoryMsgSendTracker : IMessageSendTracker
    {
        // To keep all messages to be sent
        private List<MessageSendState> _statesMaster;

        // For high performance access based on delivery tag (sequence number)
        private ConcurrentDictionary<ulong, MessageSendState> _statesByDeliveryTag;

        // For high performance access based on message id
        private ConcurrentDictionary<long, MessageSendState> _statesByMessageId;

        private bool _channelClosed;
        private string _channelClosedReason;

        public InMemoryMsgSendTracker()
        {
            _statesMaster = new List<MessageSendState>();
            _statesByDeliveryTag = new ConcurrentDictionary<ulong, MessageSendState>();
            _statesByMessageId = new ConcurrentDictionary<long, MessageSendState>();
        }

        public void OnAddMessageState(MessageSendState state)
        {
            _statesMaster.Add(state);
            _statesByMessageId.TryAdd(state.MessageId, state);
            _statesByDeliveryTag.TryAdd(state.SequenceNumber, state);
        }

        public void OnSetMessageState(MessageSendState state)
        {
            if (state.MessageId != 0)
            {
                SetStatus(state.MessageId, state.Status, state.Remark);
            }
            else
            {
                if (state.Multiple)
                {
                    SetMultipleStatus(state.SequenceNumber, state.Status);
                }
                else
                {
                    SetStatus(state.SequenceNumber, state.Status, state.Remark);
                }
            }
        }

        public void OnModelShutdown(MessageSendState state)
        {
            _channelClosed = true;
            _channelClosedReason = state.Remark;
        }

        public List<object> GetRetryItems()
        {
            _statesByMessageId.Clear();
            _statesByDeliveryTag.Clear();

            var statesMaster = new List<MessageSendState>();
            var retryList = new List<object>();
            foreach (var item in _statesMaster)
            {
                if (item.Status == SendStatus.Failed ||
                    item.Status == SendStatus.PendingSend ||
                    item.Status == SendStatus.PendingResponse)
                {
                    retryList.Add(item.MessagePayload);
                }
                else
                {
                    statesMaster.Add(item);
                }
            }
            _statesMaster = statesMaster;

            return retryList;
        }

        public IAsyncEnumerable<object> GetRetryItemsAsync()
        {
            throw new NotImplementedException();
        }

        private void SetStatus(ulong deliveryTag, SendStatus status, string remark = "")
        {
            var messageState = _statesByDeliveryTag[deliveryTag];
            InnerSetStatus(messageState, status, remark);
        }

        private void SetMultipleStatus(ulong deliveryTag, SendStatus status)
        {
            var pendingResponse = _statesMaster.Where(x => x.SequenceNumber > 0
                                            && x.SequenceNumber <= deliveryTag
                                            && x.Status == SendStatus.PendingResponse);

            foreach (var pending in pendingResponse)
                SetStatus(pending.SequenceNumber, status);
        }

        private void SetStatus(long messageId, SendStatus status, string remark = "")
        {
            var messageState = _statesByMessageId[messageId];
            InnerSetStatus(messageState, status, remark);
        }

        private void InnerSetStatus(MessageSendState messageState, SendStatus status, string remark)
        {
            if (status == SendStatus.NoExchangeFound)
            {
                //foreach (var state in _statesMaster)
                //{
                //    state.Status = status;
                //    state.Acked = true;
                //}
                messageState.Status = status;
                messageState.Acked = true;
            }
            // unroutable messages get a BasicReturn followed by a BasicAck, so we want to ignore that ack
            else if (messageState.Status != SendStatus.Unroutable)
            {
                messageState.Status = status;
                messageState.Remark = remark;
                messageState.Acked = true;
            }
        }
    }
}
