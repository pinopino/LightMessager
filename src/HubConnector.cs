using LightMessager.Common;
using LightMessager.Model;
using System;

namespace LightMessager
{
    /// <summary>
    /// 使用该类型来获取一个到底层rabbitmq的口子，通过这个口子发送消息。
    /// 通常用于一个线程绑定一个connector执行一个while(true){ ... }
    /// </summary>
    public sealed class HubConnector<TMessage> : IDisposable
        where TMessage : BaseMessage
    {
        private bool _disposed;
        private int _delay;
        private RabbitMqHub _hub;
        private PooledChannel _pooled;
        private QueueInfo _send_queue;
        private QueueInfo _route_queue;
        private QueueInfo _publish_queue;

        public bool IsDisposed { get { return _disposed; } }

        internal HubConnector(RabbitMqHub hub, IPooledWapper pooled, int delaySend)
        {
            _hub = hub;
            _delay = delaySend;
            _pooled = pooled as PooledChannel;
            EnsureQueue();
        }

        private void EnsureQueue()
        {
            _hub.EnsureSendQueue(_pooled.Channel, typeof(TMessage), _delay, out _send_queue);
            _hub.EnsureRouteQueue(_pooled.Channel, typeof(TMessage), _delay, out _route_queue);
            _hub.EnsurePublishQueue(_pooled.Channel, typeof(TMessage), _delay, out _publish_queue);
        }

        public bool Send(TMessage message, string routeKey = "")
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(HubConnector<TMessage>));

            if (string.IsNullOrWhiteSpace(message.MsgId))
                throw new ArgumentNullException("message.MsgId");

            if (!_hub.PreTrackMessage(message))
                return false;

            try
            {
                if (string.IsNullOrEmpty(routeKey))
                    _pooled.Publish(message, string.Empty, _delay == 0 ? _send_queue.Queue : _send_queue.Delay_Queue);
                else
                    _pooled.Publish(message, _delay == 0 ? _route_queue.Exchange : _route_queue.Delay_Exchange, routeKey);

                return true;
            }
            catch
            {
                _pooled.Dispose();
            }

            return false;
        }

        public bool Send(TMessage message, Func<TMessage, string> routeKeySelector)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(HubConnector<TMessage>));

            if (string.IsNullOrWhiteSpace(message.MsgId))
                throw new ArgumentNullException("message.MsgId");

            if (!_hub.PreTrackMessage(message))
                return false;

            try
            {
                var routekey = routeKeySelector(message);
                _pooled.Publish(message, _delay == 0 ? _route_queue.Exchange : _route_queue.Delay_Exchange, routekey);

                return true;
            }
            catch
            {
                _pooled.Dispose();
            }

            return false;
        }

        public bool Publish(TMessage message)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(HubConnector<TMessage>));

            if (string.IsNullOrWhiteSpace(message.MsgId))
                throw new ArgumentNullException("message.MsgId");

            if (!_hub.PreTrackMessage(message))
                return false;

            try
            {
                if (_delay == 0)
                    _pooled.Publish(message, _publish_queue.Exchange, string.Empty);
                else
                    _pooled.Publish(message, string.Empty, _publish_queue.Delay_Queue);

                return true;
            }
            catch
            {
                _pooled.Dispose();
            }

            return false;
        }

        public void Dispose()
        {
            // 必须为true
            Dispose(true);
            // 通知垃圾回收机制不再调用终结器（析构器）
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                // 清理托管资源
                _pooled.Dispose();
            }

            // 清理非托管资源

            // 让类型知道自己已经被释放
            _disposed = true;
        }
    }
}
