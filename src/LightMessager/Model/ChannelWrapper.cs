using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Text;

namespace LightMessager.Model
{
    public class ChannelWrapper
    {
        private long _channelId;
        private IModel _innerChannel;
        private RabbitMqHub _rabbitMqHub;
        private bool _publishConfirm;
        private TimeSpan? _confirmTimeout;

        internal ChannelWrapper(
            RabbitMqHub rabbitMqHub,
            bool publishConfirm,
            TimeSpan? confirmTimeout)
        {
            _rabbitMqHub = rabbitMqHub;
            _publishConfirm = publishConfirm;
            _confirmTimeout = confirmTimeout;

            InitChannel();
        }

        public long ChannelId { get { return this._channelId; } }
        internal IModel Channel { get { return this._innerChannel; } }
        public bool IsClosed { get { return this._innerChannel == null || this._innerChannel.IsClosed; } }

        internal void InitChannel()
        {
            _channelId = _rabbitMqHub.GetChannelId();
            _innerChannel = _rabbitMqHub.connection.CreateModel();
            if (_publishConfirm)
            {
                _innerChannel.ConfirmSelect();
                _innerChannel.BasicAcks += Channel_BasicAcks;
                _innerChannel.BasicNacks += Channel_BasicNacks;
                _innerChannel.BasicReturn += Channel_BasicReturn;
            }
            _innerChannel.ModelShutdown += Channel_ModelShutdown;
        }

        public void Publish(object message, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers)
        {
            /*
             * 说明：
             * .Enable publisher confirms on a channel
             * .For every published message, add a map entry that maps current sequence number to the message
             * .When a positive ack arrives, remove the entry
             * .When a negative ack arrives, remove the entry and schedule its message for republishing (or something else that's suitable)
             * 
             */
            var state = new MessageSendState(_innerChannel.NextPublishSeqNo, message);
            state.ChannelId = _channelId;
            state.Status = SendStatus.PendingResponse;
            _rabbitMqHub.OnMessageSending(state);
            InnerPublish(state, exchange, routeKey, mandatory, headers);
        }

        private void InnerPublish(MessageSendState state, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers)
        {
            var json = JsonConvert.SerializeObject(state.MessagePayload);
            var bytes = Encoding.UTF8.GetBytes(json);
            var props = _innerChannel.CreateBasicProperties();
            props.MessageId = state.MessageId.ToString();
            props.ContentType = "application/json";
            props.DeliveryMode = 2;
            props.Headers = headers;
            try
            {
                _innerChannel.BasicPublish(exchange, routeKey, mandatory, props, bytes);
            }
            catch (OperationInterruptedException ex)
            {
                if (ex.ShutdownReason.ReplyCode == 404)
                {
                    state.Status = SendStatus.NoExchangeFound;
                    state.Remark = $"Reply Code: {ex.ShutdownReason.ReplyCode}, Reply Text: {ex.ShutdownReason.ReplyText}";
                    _rabbitMqHub.OnMessageSendFailed(state);
                }
                else
                {
                    state.Status = SendStatus.Failed;
                    state.Remark = $"Reply Code: {ex.ShutdownReason.ReplyCode}, Reply Text: {ex.ShutdownReason.ReplyText}";
                    _rabbitMqHub.OnMessageSendFailed(state);
                }

                if (_innerChannel.IsClosed)
                    throw;
            }
            catch (Exception ex)
            {
                state.Status = SendStatus.Failed;
                state.Remark = ex.Message;
                _rabbitMqHub.OnMessageSendFailed(state);

                if (_innerChannel.IsClosed)
                    throw;
            }
        }

        public void WaitForConfirms()
        {
            if (_innerChannel == null || _innerChannel.IsClosed)
                return;

            _innerChannel.WaitForConfirms(_confirmTimeout.Value);
        }

        // 说明：broker正常接受到消息，会触发该ack事件
        private void Channel_BasicAcks(object sender, BasicAckEventArgs e)
        {
            /*
             * the broker may also set the multiple field in basic.ack to indicate 
             * that all messages up to and including the one with the sequence number 
             * have been handled.
             */
            _rabbitMqHub.OnChannelBasicAcks(this, e);
        }

        // 说明：nack的时候通常broker那里可能出了什么状况
        private void Channel_BasicNacks(object sender, BasicNackEventArgs e)
        {
            _rabbitMqHub.OnChannelBasicNacks(this, e);
        }

        // 说明：return类似于nack，不同在于return通常代表着unroutable；
        // 注意，BasicReturn之后会接一个BasicAck
        private void Channel_BasicReturn(object sender, BasicReturnEventArgs e)
        {
            _rabbitMqHub.OnChannelBasicReturn(this, e);
        }

        private void Channel_ModelShutdown(object sender, ShutdownEventArgs e)
        {
            // link: https://www.rabbitmq.com/channels.html
            if (e.ReplyCode != 200)
                _rabbitMqHub.OnModelShutdown(this, e);
        }
    }
}
