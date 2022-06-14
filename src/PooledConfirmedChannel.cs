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
using System.Threading;
using System.Threading.Tasks;

namespace LightMessager
{
    internal class PooledConfirmedChannel : IPooledWapper
    {
        private static Logger _logger = LogManager.GetLogger("PooledConfirmedChannel");

        private volatile int _reseting;
        private readonly int _spinCount;
        private TimeSpan? _confirmTimeout;
        private IConnection _connection;
        private IModel _innerChannel;
        private Lazy<ObjectPool<IPooledWapper>> _pool;
        private ObjectPool<IPooledWapper> _confirmedPool;
        private IMessageSendTracker _tracker;
        private CheckpointList _awaitingCheckpoints;

        public IModel Channel { get { return this._innerChannel; } }

        public PooledConfirmedChannel(RabbitMqHub rabbitMqHub)
        {
            _spinCount = 200;
            _confirmTimeout = rabbitMqHub.confirmTimeout;
            _connection = rabbitMqHub.connection;
            _pool = rabbitMqHub.channelPools;
            _confirmedPool = rabbitMqHub.confirmedChannelPools;
            _tracker = rabbitMqHub.sendTracker;
            _awaitingCheckpoints = new CheckpointList();
            InitChannel();
        }

        private void InitChannel()
        {
            _innerChannel = _connection.CreateModel();
            _innerChannel.ConfirmSelect();
            _innerChannel.BasicAcks += Channel_BasicAcks;
            _innerChannel.BasicNacks += Channel_BasicNacks;
            _innerChannel.BasicReturn += Channel_BasicReturn;
            _innerChannel.ModelShutdown += Channel_ModelShutdown;
        }

        internal void Publish<TBody>(Message<TBody> message, string exchange, string routeKey)
        {
            /*
             * 说明：
             * .Enable publisher confirms on a channel
             * .For every published message, add a map entry that maps current sequence number to the message
             * .When a positive ack arrives, remove the entry
             * .When a negative ack arrives, remove the entry and schedule its message for republishing (or something else that's suitable)
             * 
             */
            _tracker?.TrackMessageAsync(_innerChannel.NextPublishSeqNo, message).Wait();
            InnerPublish(message, exchange, routeKey).Wait();
        }

        internal async Task<ulong> PublishReturnSeqAsync<TBody>(Message<TBody> message, string exchange, string routeKey)
        {
            var sequence = _innerChannel.NextPublishSeqNo;
            await _tracker?.TrackMessageAsync(sequence, message);
            await InnerPublish(message, exchange, routeKey);

            return sequence;
        }

        private async Task InnerPublish<TBody>(Message<TBody> message, string exchange, string routeKey)
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
                    await _tracker?.SetErrorAsync(message, SendStatus.NoExchangeFound, remark: ex.Message);
                else
                    await _tracker?.SetErrorAsync(message, SendStatus.Failed, remark: ex.Message);

                if (_innerChannel.IsClosed)
                    throw;
            }
            catch (Exception ex)
            {
                await _tracker?.SetErrorAsync(message, SendStatus.Failed, remark: ex.Message);

                if (_innerChannel.IsClosed)
                    throw;
            }
        }

        internal bool WaitForConfirms()
        {
            var timeout = _confirmTimeout.HasValue ? _confirmTimeout.Value : TimeSpan.FromSeconds(10);
            return _innerChannel.WaitForConfirms(timeout);
        }

        internal Task<bool> WaitForConfirmsAsync(ulong deliveryTag)
        {
            while (_reseting > 0)
                Thread.SpinWait(_spinCount);

            var tcs = new TaskCompletionSource<bool>();
            _awaitingCheckpoints.AddNode(deliveryTag, tcs);
            return tcs.Task;
        }

        // 说明：broker正常接受到消息，会触发该ack事件
        private async void Channel_BasicAcks(object sender, BasicAckEventArgs e)
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
                    await _tracker.SetMultipleStatusAsync(e.DeliveryTag, SendStatus.Confirmed);
                else
                    await _tracker.SetStatusAsync(e.DeliveryTag, SendStatus.Confirmed);
            }

            SetCheckpoint(e.DeliveryTag);
        }

        // nack的时候通常broker那里可能出了什么状况，log一波（暂时不考虑重试了）
        // 消息的状态置为终结态Error
        private async void Channel_BasicNacks(object sender, BasicNackEventArgs e)
        {
            if (_tracker != null)
            {
                if (e.Multiple)
                    await _tracker.SetMultipleStatusAsync(e.DeliveryTag, SendStatus.Nacked);
                else
                    await _tracker.SetStatusAsync(e.DeliveryTag, SendStatus.Nacked, remark: string.Empty);
            }

            SetCheckpoint(e.DeliveryTag);
        }

        // 说明：return类似于nack，不同在于return通常代表着unroutable，
        // 所以log一下但并不会重试，消息的状态也直接置为终结态Error_Unroutable
        private async void Channel_BasicReturn(object sender, BasicReturnEventArgs e)
        {
            var fakeMsg = new Message<string> { Body = e.BasicProperties.MessageId };
            await _tracker?.SetErrorAsync(fakeMsg, SendStatus.Unroutable, $"Broker Return，MsgId：{fakeMsg.MsgId}，ReplyCode：{e.ReplyCode}，ReplyText：{e.ReplyText}");
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

            _awaitingCheckpoints.Check(deliveryTag);
        }

        private void ClearCheckpoint()
        {
            Interlocked.Increment(ref _reseting);
            var old = _awaitingCheckpoints;
            _awaitingCheckpoints = new CheckpointList();
            Interlocked.Decrement(ref _reseting);

            _awaitingCheckpoints.Check();
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
                if (_confirmedPool.IsDisposed)
                {
                    _innerChannel.Close();
                    _innerChannel.Dispose();
                }
                else
                {
                    // 说明：
                    // channel层面的异常会导致channel关闭并且不可以再被使用，因此在配合
                    // 池化策略时一种方式是捕获该异常并且创建一个新的channel以补充池中可用的channle；
                    if (_innerChannel.IsClosed)
                    {
                        _tracker?.ResetAsync($"Channel Shutdown，ReplyCode：{_innerChannel.CloseReason.ReplyCode}，ReplyText：{_innerChannel.CloseReason.ReplyText}").Wait();
                        ClearCheckpoint();
                        InitChannel();
                        _confirmedPool.Put(this);
                    }
                    else
                    {
                        _confirmedPool.Put(this);
                    }
                }
            }

            // 清理非托管资源
        }
    }
}
