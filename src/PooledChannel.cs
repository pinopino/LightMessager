using LightMessager.Common;
using LightMessager.Model;
using LightMessager.Track;
using Newtonsoft.Json;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Text;
using System.Threading.Tasks;

namespace LightMessager
{
    internal class PooledChannel : IPooledWapper
    {
        private object _lockobj;
        private IModel _innerChannel;
        private IConnection _connection;
        private BaseMessageTracker _tracker;
        private ObjectPool<IPooledWapper> _pool;
        private static Logger _logger = LogManager.GetLogger("PooledChannel");

        public IModel Channel { get { return this._innerChannel; } }

        public PooledChannel(IModel channel, ObjectPool<IPooledWapper> pool,
            BaseMessageTracker tracker, IConnection connection)
        {
            _lockobj = new object();
            _pool = pool;
            _tracker = tracker;
            _innerChannel = channel;
            _connection = connection;
            InitChannel();
        }

        private void InitChannel()
        {
            _innerChannel.ConfirmSelect();
            _innerChannel.BasicAcks += Channel_BasicAcks;
            _innerChannel.BasicNacks += Channel_BasicNacks;
            _innerChannel.BasicReturn += Channel_BasicReturn;
            _innerChannel.ModelShutdown += Channel_ModelShutdown;
        }

        internal void Publish(BaseMessage message, string exchange, string routeKey)
        {
            _tracker.TrackMessage(_innerChannel.NextPublishSeqNo, message.MsgId);
            InnerPublish(message, exchange, routeKey);
        }

        internal ulong PublishAsync(BaseMessage message, string exchange, string routeKey)
        {
            var sequence = _innerChannel.NextPublishSeqNo;
            _tracker.TrackMessage(sequence, message.MsgId);
            InnerPublish(message, exchange, routeKey);
            return sequence;
        }

        private void InnerPublish(BaseMessage message, string exchange, string routeKey)
        {
            var json = JsonConvert.SerializeObject(message);
            var bytes = Encoding.UTF8.GetBytes(json);
            var props = _innerChannel.CreateBasicProperties();
            props.MessageId = message.MsgId;
            props.ContentType = "text/plain";
            props.DeliveryMode = 2;
            try
            {
                _innerChannel.BasicPublish(exchange, routeKey, mandatory: true, props, bytes);
            }
            catch (OperationInterruptedException ex)
            {
                if (ex.ShutdownReason.ReplyCode == 404)
                    _tracker.SetStatus(message.MsgId, MessageState.Error_NoExchangeFound, remark: ex.Message);
                else
                    _tracker.SetStatus(message.MsgId, MessageState.Error, remark: ex.Message);
                throw;
            }
            catch (Exception ex)
            {
                _tracker.SetStatus(message.MsgId, MessageState.Error, remark: ex.Message);
                throw;
            }
        }

        internal bool WaitForConfirms()
        {
            return _innerChannel.WaitForConfirms(TimeSpan.FromSeconds(10));
        }

        internal Task WaitForConfirmsAsync(ulong deliveryTag, string msgId)
        {
            return _tracker.RegisterMapForAsync(deliveryTag, msgId);
        }

        // 说明：broker正常接受到消息，会触发该ack事件
        private void Channel_BasicAcks(object sender, BasicAckEventArgs e)
        {
            /*
             * the broker may also set the multiple field in basic.ack to indicate 
             * that all messages up to and including the one with the sequence number 
             * have been handled.
             */
            // 说明：如这里的备注信息，因此我们需要区别处理e.Multiple的两种情况；
            // 另外，在手动等待confirm的模式中我们可以直接调用waitForConfirms(timeout)，
            // 放到这里基于事件回调的方式下timeout还是有一定意义的，可以考虑自己实现
            if (e.Multiple)
                _tracker.SetMultipleStatus(e.DeliveryTag, MessageState.Confirmed);
            else
                _tracker.SetStatus(e.DeliveryTag, MessageState.Confirmed);
        }

        // nack的时候通常broker那里可能出了什么状况，log一波（暂时不考虑重试了）
        // 消息的状态置为终结态Error
        private void Channel_BasicNacks(object sender, BasicNackEventArgs e)
        {
            if (e.Multiple)
                _tracker.SetMultipleStatus(e.DeliveryTag, MessageState.Error);
            else
                _tracker.SetStatus(e.DeliveryTag, MessageState.Error, remark: string.Empty);
        }

        // 说明：return类似于nack，不同在于return通常代表着unroutable，
        // 所以log一下但并不会重试，消息的状态也直接置为终结态Error_Unroutable
        private void Channel_BasicReturn(object sender, BasicReturnEventArgs e)
        {
            // TODO：return之后还会再接一次ack，消息本地消息的状态被错误重置
            _tracker.SetStatus(e.BasicProperties.MessageId, MessageState.Error_Unroutable,
                remark: $"Broker Return，MsgId：{e.BasicProperties.MessageId}，ReplyCode：{e.ReplyCode}，ReplyText：{e.ReplyText}");
        }

        private void Channel_ModelShutdown(object sender, ShutdownEventArgs e)
        {
            // link: https://www.rabbitmq.com/channels.html
            _logger.Warn($"Channel Shutdown，ReplyCode：{e.ReplyCode}，ReplyText：{e.ReplyText}");
        }

        public void Dispose()
        {
            // 必须为true
            Dispose(true);
            // 通知垃圾回收机制不再调用终结器（析构器）
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // 清理托管资源
                if (_pool.IsDisposed)
                {
                    _innerChannel.Dispose();
                }
                else
                {
                    // 说明：
                    // channel层面的异常会导致channel关闭并且不可以再被使用，因此在配合
                    // 池化策略时一种方式是捕获该异常并且创建一个新的channel以补充池中可用的channle；
                    if (_innerChannel.IsClosed)
                    {
                        _tracker.Reset();
                        _innerChannel = _connection.CreateModel();
                        InitChannel();
                        _pool.Put(this);
                    }
                    else
                    {
                        _pool.Put(this);
                    }
                }
            }

            // 清理非托管资源
        }
    }
}
