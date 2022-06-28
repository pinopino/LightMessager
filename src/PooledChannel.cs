using LightMessager.Common;
using LightMessager.Model;
using Newtonsoft.Json;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace LightMessager
{
    internal class PooledChannel : IPooledWapper
    {
        private static Logger _logger = LogManager.GetLogger("PooledChannel");

        private IConnection _connection;
        private IModel _innerChannel;
        private Lazy<ObjectPool<IPooledWapper>> _pool;
        private RabbitMqHub _rabbitMqHub;

        public IModel Channel { get { return this._innerChannel; } }

        public PooledChannel(RabbitMqHub rabbitMqHub)
        {
            _rabbitMqHub = rabbitMqHub;
            _connection = rabbitMqHub.connection;
            _pool = rabbitMqHub.channelPools;
            InitChannel();
        }

        private void InitChannel()
        {
            _innerChannel = _connection.CreateModel();
            _innerChannel.ModelShutdown += Channel_ModelShutdown;
        }

        internal void Publish<TBody>(Message<TBody> message, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers)
        {
            _rabbitMqHub.OnMessageSending(_innerChannel.NextPublishSeqNo, message);
            InnerPublish(message, exchange, routeKey, mandatory, headers);
        }

        internal Task PublishAsync<TBody>(Message<TBody> message, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers)
        {
            _rabbitMqHub.OnMessageSending(_innerChannel.NextPublishSeqNo, message);
            return Task.Factory.StartNew(() => InnerPublish(message, exchange, routeKey, mandatory, headers));
        }

        private void InnerPublish<TBody>(Message<TBody> message, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers)
        {
            var json = JsonConvert.SerializeObject(message);
            var bytes = Encoding.UTF8.GetBytes(json);
            var props = _innerChannel.CreateBasicProperties();
            props.MessageId = message.MsgId;
            props.ContentType = "text/plain";
            props.DeliveryMode = 2;
            props.Headers = headers;
            try
            {
                _innerChannel.BasicPublish(exchange, routeKey, mandatory, props, bytes);
            }
            catch (OperationInterruptedException ex)
            {
                if (ex.ShutdownReason.ReplyCode == 404)
                    _rabbitMqHub.OnMessageSendFailed(message, SendStatus.NoExchangeFound, remark: ex.Message);
                else
                    _rabbitMqHub.OnMessageSendFailed(message, SendStatus.Failed, remark: ex.Message);

                if (_innerChannel.IsClosed)
                    throw;
            }
            catch (Exception ex)
            {
                _rabbitMqHub.OnMessageSendFailed(message, SendStatus.Failed, remark: ex.Message);

                if (_innerChannel.IsClosed)
                    throw;
            }
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
                if (_pool.Value.IsDisposed)
                {
                    _innerChannel.Close();
                    _innerChannel.Dispose();
                }
                else
                {
                    if (_innerChannel.IsClosed)
                    {
                        InitChannel();
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
