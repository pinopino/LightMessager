using LightMessager.Common;
using LightMessager.Model;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;

namespace LightMessager
{
    internal class PooledConfirmChannel : IPooledWrapper
    {
        private static Logger _logger = LogManager.GetLogger("PooledConfirmedChannel");

        private ChannelWrapper _channelWrapper;
        private Lazy<ObjectPool<IPooledWrapper>> _confirmedPool;

        public IModel Channel { get { return this._channelWrapper.Channel; } }

        public PooledConfirmChannel(ChannelWrapper channelWrapper, Lazy<ObjectPool<IPooledWrapper>> pool)
        {
            _channelWrapper = channelWrapper;
            _confirmedPool = pool;
        }

        internal void Publish(object message, string exchange, string routeKey, bool mandatory = true, IDictionary<string, object> headers = null)
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

        internal void WaitForConfirms()
        {
            _channelWrapper.WaitForConfirms();
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
                if (_confirmedPool.Value.IsDisposed)
                {
                    Channel?.Close();
                    Channel?.Dispose();
                }
                else
                {
                    // 说明：
                    // channel层面的异常会导致channel关闭并且不可以再被使用，因此在配合
                    // 池化策略时一种方式是捕获该异常并且创建一个新的channel以补充池中可用的channle；
                    if (Channel != null && Channel.IsClosed)
                    {
                        _channelWrapper.InitChannel();
                        _confirmedPool.Value.Put(this);
                    }
                    else
                    {
                        _confirmedPool.Value.Put(this);
                    }
                }
            }

            // 清理非托管资源
        }
    }
}
