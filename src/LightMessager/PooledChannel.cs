using LightMessager.Common;
using LightMessager.Model;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;

namespace LightMessager
{
    internal class PooledChannel : IPooledWrapper
    {
        private static Logger _logger = LogManager.GetLogger("PooledChannel");

        private ChannelWrapper _channelWrapper;
        private Lazy<ObjectPool<IPooledWrapper>> _pool;

        internal IModel Channel { get { return this._channelWrapper.Channel; } }

        internal PooledChannel(ChannelWrapper channelWrapper, Lazy<ObjectPool<IPooledWrapper>> pool)
        {
            _channelWrapper = channelWrapper;
            _pool = pool;
        }

        internal void Publish(object message, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers)
        {
            try
            {
                _channelWrapper.Publish(message, exchange, routeKey, mandatory, headers);
            }
            catch (OperationInterruptedException ex)
            {
                this.Dispose();
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
                if (_pool.Value.IsDisposed)
                {
                    Channel?.Close();
                    Channel?.Dispose();
                }
                else
                {
                    if (Channel != null && Channel.IsClosed)
                    {
                        _channelWrapper.InitChannel();
                        _pool.Value.Put(this);
                    }
                    else
                    {
                        _pool.Value.Put(this);
                    }
                }
            }

            // 清理非托管资源
        }
    }
}
