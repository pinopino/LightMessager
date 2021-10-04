using LightMessager.Common;
using LightMessager.Model;
using LightMessager.Track;
using Newtonsoft.Json;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LightMessager
{
    internal class PooledChannel : IPooledWapper
    {
        private static Logger _logger = LogManager.GetLogger("PooledChannel");

        private readonly int _spinCount;
        private volatile int _reseting;
        private IModel _innerChannel;
        private IConnection _connection;
        private IMessageSendTracker _tracker;
        private ObjectPool<IPooledWapper> _pool;
        private ConcurrentDictionary<ulong, TaskCompletionSource<bool>> _awaitingCheckpoint;

        public IModel Channel { get { return this._innerChannel; } }

        public PooledChannel(IModel channel, ObjectPool<IPooledWapper> pool, IConnection connection, bool usingTrack = true)
        {
            _pool = pool;
            _spinCount = 200;
            if (usingTrack)
                _tracker = new InMemoryTracker();
            else
                _awaitingCheckpoint = new ConcurrentDictionary<ulong, TaskCompletionSource<bool>>();
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

        internal void Publish<TBody>(Message<TBody> message, string exchange, string routeKey)
        {
            _tracker?.TrackMessage(_innerChannel.NextPublishSeqNo, message);
            InnerPublish(message, exchange, routeKey);
        }

        internal async ValueTask<ulong> PublishReturnSeqAsync<TBody>(Message<TBody> message, string exchange, string routeKey)
        {
            var sequence = _innerChannel.NextPublishSeqNo;
            if (_tracker != null)
                await _tracker.TrackMessageAsync(sequence, message);
            InnerPublish(message, exchange, routeKey);
            return sequence;
        }

        private void InnerPublish<TBody>(Message<TBody> message, string exchange, string routeKey)
        {
            var json = JsonConvert.SerializeObject(message);
            var bytes = Encoding.UTF8.GetBytes(json);
            var props = _innerChannel.CreateBasicProperties();
            if (typeof(IIdentifiedMessage).IsAssignableFrom(typeof(TBody)))
            {
                props.MessageId = (message.Body as IIdentifiedMessage).MsgId;
            }
            props.ContentType = "text/plain";
            props.DeliveryMode = 2;
            try
            {
                _innerChannel.BasicPublish(exchange, routeKey, mandatory: true, props, bytes);
            }
            catch (OperationInterruptedException ex)
            {
                if (ex.ShutdownReason.ReplyCode == 404)
                    _tracker?.SetStatus(message, SendStatus.NoExchangeFound, remark: ex.Message);
                else
                    _tracker?.SetStatus(message, SendStatus.Failed, remark: ex.Message);

                if (_innerChannel.IsClosed)
                    throw;
            }
            catch (Exception ex)
            {
                _tracker?.SetStatus(message, SendStatus.Failed, remark: ex.Message);

                if (_innerChannel.IsClosed)
                    throw;
            }
        }

        internal bool WaitForConfirms()
        {
            return _innerChannel.WaitForConfirms(TimeSpan.FromSeconds(10));
        }

        internal Task<bool> WaitForConfirmsAsync(ulong deliveryTag)
        {
            while (_reseting > 0)
                Thread.SpinWait(_spinCount);

            var tcs = new TaskCompletionSource<bool>();
            _awaitingCheckpoint.TryAdd(deliveryTag, tcs);
            return tcs.Task;
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
            if (_tracker != null)
            {
                if (e.Multiple)
                    _tracker.SetMultipleStatus(e.DeliveryTag, SendStatus.Confirmed);
                else
                    _tracker.SetStatus(e.DeliveryTag, SendStatus.Confirmed);
            }

            SetCheckpoint(e.DeliveryTag);
        }

        // nack的时候通常broker那里可能出了什么状况，log一波（暂时不考虑重试了）
        // 消息的状态置为终结态Error
        private void Channel_BasicNacks(object sender, BasicNackEventArgs e)
        {
            if (_tracker != null)
            {
                if (e.Multiple)
                    _tracker.SetMultipleStatus(e.DeliveryTag, SendStatus.Nacked);
                else
                    _tracker.SetStatus(e.DeliveryTag, SendStatus.Nacked, remark: string.Empty);
            }

            SetCheckpoint(e.DeliveryTag);
        }

        // 说明：return类似于nack，不同在于return通常代表着unroutable，
        // 所以log一下但并不会重试，消息的状态也直接置为终结态Error_Unroutable
        private void Channel_BasicReturn(object sender, BasicReturnEventArgs e)
        {
            var fake = new Message<string> { Body = e.BasicProperties.MessageId };
            _tracker?.SetStatus(fake, SendStatus.Unroutable, $"Broker Return，MsgId：{e.BasicProperties.MessageId}，ReplyCode：{e.ReplyCode}，ReplyText：{e.ReplyText}");
        }

        private void Channel_ModelShutdown(object sender, ShutdownEventArgs e)
        {
            // link: https://www.rabbitmq.com/channels.html
            _logger.Warn($"Channel Shutdown，ReplyCode：{e.ReplyCode}，ReplyText：{e.ReplyText}");
        }

        private void SetCheckpoint(ulong deliveryTag)
        {
            while (_reseting > 0)
                Thread.SpinWait(_spinCount);

            if (_awaitingCheckpoint.TryRemove(deliveryTag, out TaskCompletionSource<bool> val))
            {
                val.SetResult(true);
            }
        }

        private void ClearCheckpoint()
        {
            Interlocked.Increment(ref _reseting);
            var old = _awaitingCheckpoint;
            _awaitingCheckpoint = new ConcurrentDictionary<ulong, TaskCompletionSource<bool>>();
            Interlocked.Decrement(ref _reseting);

            foreach (var item in old)
            {
                item.Value.SetResult(false);
            }
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
                        _innerChannel = _connection.CreateModel();
                        _tracker?.Reset($"Channel Shutdown，ReplyCode：{_innerChannel.CloseReason.ReplyCode}，ReplyText：{_innerChannel.CloseReason.ReplyText}");
                        ClearCheckpoint();
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
