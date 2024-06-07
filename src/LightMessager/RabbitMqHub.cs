using LightMessager.Common;
using LightMessager.Model;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

[assembly: InternalsVisibleTo("LightMessager.Test")]
namespace LightMessager
{
    /// <summary>
    /// A lightweight wrapper for RabbitMQ IConnection
    /// </summary>
    public sealed partial class RabbitMqHub
    {
        private Logger _logger = LogManager.GetLogger("RabbitMqHub");

        private int _batchSize;
        private ushort _prefetchCount;
        private TimeSpan _defaultExpiry;
        private MemoryCache _metaInfo;

        internal Lazy<ObjectPool<IPooledWrapper>> channelPools;
        internal Lazy<ObjectPool<IPooledWrapper>> confirmChannelPools;
        internal TimeSpan confirmTimeout;
        internal IConnection connection;
        internal IConnection asynConnection;
        internal static long _number = -1; // channel编号

        public Advanced Advance { private set; get; }
        // TODO：async event
        //
        // links：
        // https://stackoverflow.com/questions/19415646/should-i-avoid-async-void-event-handlers
        // https://stackoverflow.com/questions/12451609/how-to-await-raising-an-eventhandler-event
        // send
        public event EventHandler<MessageSendState> MessageSending;
        public event EventHandler<MessageSendState> MessageSendOK;
        public event EventHandler<MessageSendState> MessageSendFailed;
        // recv/consume
        public event EventHandler<MessageConsumeState> MessageReceived;
        public event EventHandler<MessageConsumeState> MessageConsumeOK;
        public event EventHandler<MessageConsumeState> MessageConsumeFailed;

        public RabbitMqHub()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();
            Init(configuration);

            RefreshNumber();
        }

        public RabbitMqHub(IConfigurationRoot configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            Init(configuration);

            RefreshNumber();
        }

        private void Init(IConfigurationRoot configuration)
        {
            InitConnection(configuration);
            InitAsyncConnection(configuration);
            InitChannelPool();
            InitConfirmedChannelPool();
            InitOther(configuration);
            InitAdvance();
        }

        #region 初始化
        private void InitConnection(IConfigurationRoot configuration)
        {
            var factory = GetConnectionFactory(configuration);
            connection = factory.CreateConnection();
        }

        private void InitAsyncConnection(IConfigurationRoot configuration)
        {
            var factory = GetConnectionFactory(configuration);
            factory.DispatchConsumersAsync = true;
            asynConnection = factory.CreateConnection();
        }

        private ConnectionFactory GetConnectionFactory(IConfigurationRoot configuration)
        {
            var factory = new ConnectionFactory();
            factory.UserName = configuration.GetSection("LightMessager:UserName").Value;
            factory.Password = configuration.GetSection("LightMessager:Password").Value;
            factory.VirtualHost = configuration.GetSection("LightMessager:VirtualHost").Value;
            factory.HostName = configuration.GetSection("LightMessager:HostName").Value;
            factory.Port = int.Parse(configuration.GetSection("LightMessager:Port").Value);
            factory.AutomaticRecoveryEnabled = true;
            factory.NetworkRecoveryInterval = TimeSpan.FromSeconds(10);

            return factory;
        }

        private void InitChannelPool()
        {
            var min = 4;
            var cpu = Math.Max(Environment.ProcessorCount, min);
            channelPools = new Lazy<ObjectPool<IPooledWrapper>>(() =>
                new ObjectPool<IPooledWrapper>(p => new PooledChannel(CreateChannel(false), this.channelPools), min, cpu));
        }

        private void InitConfirmedChannelPool()
        {
            var min = 4;
            var cpu = Math.Max(Environment.ProcessorCount, min);
            confirmChannelPools = new Lazy<ObjectPool<IPooledWrapper>>(() =>
                new ObjectPool<IPooledWrapper>(p => new PooledConfirmChannel(CreateChannel(true), this.confirmChannelPools), min, cpu));
        }

        private void InitAdvance()
        {
            Advance = new Advanced(this);
        }

        private void InitOther(IConfigurationRoot configuration)
        {
            _batchSize = 300;
            _prefetchCount = 200;
            _defaultExpiry = TimeSpan.FromSeconds(30);
            confirmTimeout = TimeSpan.FromSeconds(10);
            _metaInfo = new MemoryCache(new MemoryCacheOptions());
        }
        #endregion

        #region 发送端事件
        internal void OnMessageSending(MessageSendState state)
        {
            // Make a temporary copy of the event to avoid possibility of
            // a race condition if the last subscriber unsubscribes
            // immediately after the null check and before the event is raised.
            // 下同。
            var raiseEvent = MessageSending;
            raiseEvent?.Invoke(this, state);
        }

        internal void OnMessageSendFailed(MessageSendState state)
        {
            var raiseEvent = MessageSendFailed;
            raiseEvent?.Invoke(this, state);
        }

        internal void OnChannelBasicAcks(object sender, BasicAckEventArgs e)
        {
            var raiseEvent = MessageSendOK;
            raiseEvent?.Invoke(this, new MessageSendState
            {
                ChannelId = ((ChannelWrapper)sender).ChannelId,
                SequenceNumber = e.DeliveryTag,
                Multiple = e.Multiple,
                Status = SendStatus.Success,
                Acked = true
            });
        }

        internal void OnChannelBasicNacks(object sender, BasicNackEventArgs e)
        {
            var raiseEvent = MessageSendFailed;
            raiseEvent?.Invoke(this, new MessageSendState
            {
                ChannelId = ((ChannelWrapper)sender).ChannelId,
                SequenceNumber = e.DeliveryTag,
                Multiple = e.Multiple,
                Status = SendStatus.Failed,
                Acked = true
            });
        }

        internal void OnChannelBasicReturn(object sender, BasicReturnEventArgs e)
        {
            var raiseEvent = MessageSendFailed;
            raiseEvent?.Invoke(this, new MessageSendState
            {
                ChannelId = ((ChannelWrapper)sender).ChannelId,
                MessageId = long.Parse(e.BasicProperties.MessageId),
                Status = SendStatus.Unroutable,
                Remark = $"Reply Code: {e.ReplyCode}, Reply Text: {e.ReplyText}"
            });
        }

        internal void OnModelShutdown(object sender, ShutdownEventArgs e)
        {
            var raiseEvent = MessageSendFailed;
            raiseEvent?.Invoke(this, new MessageSendState
            {
                ChannelId = ((ChannelWrapper)sender).ChannelId,
                Remark = $"Reply Code: {e.ReplyCode}, Reply Text: {e.ReplyText}"
            });
        }

        internal long GetChannelId()
        {
            return Interlocked.Increment(ref _number);
        }

        private void RefreshNumber()
        {
            Interlocked.Increment(ref _number);
        }
        #endregion

        #region 接收端事件
        internal void OnMessageReceived(MessageConsumeState state)
        {
            var raiseEvent = MessageReceived;
            raiseEvent?.Invoke(this, state);
        }

        internal void OnMessageConsumeOK(MessageConsumeState state)
        {
            var raiseEvent = MessageConsumeOK;
            raiseEvent?.Invoke(this, state);
        }

        internal void OnMessageConsumeFailed(MessageConsumeState state)
        {
            var raiseEvent = MessageConsumeFailed;
            raiseEvent?.Invoke(this, state);
        }
        #endregion

        internal ChannelWrapper CreateChannel(bool publishConfirm)
        {
            return new ChannelWrapper(this, publishConfirm, publishConfirm ? this.confirmTimeout : null);
        }

        internal IPooledWrapper GetChannelFromPool()
        {
            var pool = channelPools.Value;
            return pool.Get();
        }

        internal IPooledWrapper GetConfirmChannelFromPool()
        {
            var pool = confirmChannelPools.Value;
            return pool.Get();
        }

        #region 确保消息队列存在
        /// <summary>
        /// send方式的生产端
        /// </summary>
        internal void EnsureSend(IModel channel, Type messageType, int delay, out QueueInfo info, out DelayQueueInfo delayInfo)
        {
            var typeName = GetTypeName(messageType);
            var key = typeName;
            if (!_metaInfo.TryGetValue(key, out info))
            {
                info = new QueueInfo { Exchange = string.Empty, Queue = key };
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);

                _metaInfo.Set(key, info, _defaultExpiry);
            }

            delayInfo = null;
            if (delay > 0)
            {
                // links:
                // https://www.rabbitmq.com/ttl.html
                // https://www.rabbitmq.com/dlx.html
                var dlxKey = $"{key}.delay_{delay}";
                if (!_metaInfo.TryGetValue(dlxKey, out delayInfo))
                {
                    delayInfo = new DelayQueueInfo();
                    delayInfo.DelayQueue = dlxKey;

                    var args = new Dictionary<string, object>();
                    args.Add("x-expires", delay * 2 * 1000);
                    args.Add("x-message-ttl", delay * 1000);
                    args.Add("x-dead-letter-exchange", info.Exchange);
                    args.Add("x-dead-letter-routing-key", info.Queue);
                    channel.QueueDeclare(
                        delayInfo.DelayQueue,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: args);

                    _metaInfo.Set(dlxKey, delayInfo, TimeSpan.FromSeconds(delay * 1.5));
                }
            }
        }

        /// <summary>
        /// send方式的消费端
        /// </summary>
        internal void EnsureSendQueue(IModel channel, Type messageType, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_metaInfo.TryGetValue(key, out info))
            {
                info = new QueueInfo { Exchange = string.Empty, Queue = key };
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);

                _metaInfo.Set(key, info, _defaultExpiry);
            }
        }

        /// <summary>
        /// send with route方式的生产端
        /// </summary>
        internal void EnsureRoute(IModel channel, Type messageType, string routeKey, int delay, out QueueInfo info, out DelayQueueInfo delayInfo)
        {
            EnsureRouteExchange(channel, messageType, out info);

            delayInfo = null;
            if (delay > 0)
            {
                var typeName = GetTypeName(messageType);
                var dlxKey = GetDelayKey(typeName, ExchangeType.Direct, delay);
                if (!_metaInfo.TryGetValue(dlxKey, out delayInfo))
                {
                    delayInfo = new DelayQueueInfo();
                    delayInfo.DelayQueue = GetSubscriberName(typeName, ExchangeType.Direct, $"delay_{delay}");

                    var args = new Dictionary<string, object>();
                    args.Add("x-expires", delay * 2 * 1000);
                    args.Add("x-message-ttl", delay * 1000);
                    args.Add("x-dead-letter-exchange", info.Exchange);
                    args.Add("x-dead-letter-routing-key", routeKey);
                    channel.QueueDeclare(
                        delayInfo.DelayQueue,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: args);

                    _metaInfo.Set(dlxKey, delayInfo, TimeSpan.FromSeconds(delay * 1.5));
                }
            }
        }

        /// <summary>
        /// send with route方式的消费端
        /// </summary>
        internal void EnsureRouteExchange(IModel channel, Type messageType, out QueueInfo info)
        {
            var typeName = GetTypeName(messageType);
            var key = GetExchangeName(typeName, ExchangeType.Direct);
            if (!_metaInfo.TryGetValue(key, out info))
            {
                info = new QueueInfo { Exchange = key, Queue = string.Empty };
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Direct, durable: true, autoDelete: false);

                _metaInfo.Set(key, info, _defaultExpiry);
            }
        }

        /// <summary>
        /// send with route方式的消费端
        /// </summary>
        internal void EnsureRouteQueue(IModel channel, Type messageType, string subscriber, string[] subscribeKeys, out QueueInfo info)
        {
            var typeName = GetTypeName(messageType);
            var key = GetSubscriberName(typeName, ExchangeType.Direct, subscriber);
            if (!_metaInfo.TryGetValue(key, out info))
            {
                info = new QueueInfo { Exchange = GetExchangeName(typeName, ExchangeType.Direct), Queue = key };
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Direct, durable: true, autoDelete: false);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
                for (var i = 0; i < subscribeKeys.Length; i++)
                    channel.QueueBind(info.Queue, info.Exchange, routingKey: subscribeKeys[i]);

                _metaInfo.Set(key, info, _defaultExpiry);
            }
        }

        /// <summary>
        /// send with topic方式的生产端
        /// </summary>
        internal void EnsureTopic(IModel channel, Type messageType, string routeKey, int delay, out QueueInfo info, out DelayQueueInfo delayInfo)
        {
            EnsureTopicExchange(channel, messageType, out info);

            delayInfo = null;
            if (delay > 0)
            {
                var typeName = GetTypeName(messageType);
                var dlxKey = GetDelayKey(typeName, ExchangeType.Topic, delay);
                if (!_metaInfo.TryGetValue(dlxKey, out delayInfo))
                {
                    delayInfo = new DelayQueueInfo();
                    delayInfo.DelayQueue = GetSubscriberName(typeName, ExchangeType.Topic, $"delay_{delay}");

                    var args = new Dictionary<string, object>();
                    args.Add("x-expires", delay * 2 * 1000);
                    args.Add("x-message-ttl", delay * 1000);
                    args.Add("x-dead-letter-exchange", info.Exchange);
                    args.Add("x-dead-letter-routing-key", routeKey);
                    channel.QueueDeclare(
                        delayInfo.DelayQueue,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: args);

                    _metaInfo.Set(dlxKey, delayInfo, TimeSpan.FromSeconds(delay * 1.5));
                }
            }
        }

        /// <summary>
        /// send with topic方式的消费端
        /// </summary>
        internal void EnsureTopicExchange(IModel channel, Type messageType, out QueueInfo info)
        {
            var typeName = GetTypeName(messageType);
            var key = GetExchangeName(typeName, ExchangeType.Topic);
            if (!_metaInfo.TryGetValue(key, out info))
            {
                info = new QueueInfo { Exchange = key, Queue = string.Empty };
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Topic, durable: true, autoDelete: false);

                _metaInfo.Set(key, info, _defaultExpiry);
            }
        }

        /// <summary>
        /// send with topic方式的消费端
        /// </summary>
        internal void EnsureTopicQueue(IModel channel, Type messageType, string subscriber, string[] subscribeKeys, out QueueInfo info)
        {
            var typeName = GetTypeName(messageType);
            var key = GetSubscriberName(typeName, ExchangeType.Topic, subscriber);
            if (!_metaInfo.TryGetValue(key, out info))
            {
                info = new QueueInfo { Exchange = GetExchangeName(typeName, ExchangeType.Topic), Queue = key };
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Topic, durable: true, autoDelete: false);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
                for (var i = 0; i < subscribeKeys.Length; i++)
                    channel.QueueBind(info.Queue, info.Exchange, routingKey: subscribeKeys[i]);

                _metaInfo.Set(key, info, _defaultExpiry);
            }
        }

        /// <summary>
        /// publish（fanout）方式的生产端
        /// </summary>
        internal void EnsurePublish(IModel channel, Type messageType, int delay, out QueueInfo info, out DelayQueueInfo delayInfo)
        {
            EnsurePublishExchange(channel, messageType, out info);

            delayInfo = null;
            if (delay > 0)
            {
                var typeName = GetTypeName(messageType);
                var dlxKey = GetDelayKey(typeName, ExchangeType.Fanout, delay);
                if (!_metaInfo.TryGetValue(dlxKey, out delayInfo))
                {
                    delayInfo = new DelayQueueInfo();
                    delayInfo.DelayQueue = GetSubscriberName(typeName, ExchangeType.Fanout, $"delay_{delay}");

                    var args = new Dictionary<string, object>();
                    args.Add("x-expires", delay * 2 * 1000);
                    args.Add("x-message-ttl", delay * 1000);
                    args.Add("x-dead-letter-exchange", info.Exchange);
                    channel.QueueDeclare(
                        delayInfo.DelayQueue,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: args);

                    _metaInfo.Set(dlxKey, delayInfo, TimeSpan.FromSeconds(delay * 1.5));
                }
            }
        }

        /// <summary>
        /// publish（fanout）方式的消费端
        /// </summary>
        internal void EnsurePublishExchange(IModel channel, Type messageType, out QueueInfo info)
        {
            var typeName = GetTypeName(messageType);
            var key = GetExchangeName(typeName, ExchangeType.Fanout);
            if (!_metaInfo.TryGetValue(key, out info))
            {
                info = new QueueInfo { Exchange = key, Queue = string.Empty };
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Fanout, durable: true, autoDelete: false);

                _metaInfo.Set(key, info, _defaultExpiry);
            }
        }

        /// <summary>
        /// publish（fanout）方式的消费端
        /// </summary>
        internal void EnsurePublishQueue(IModel channel, Type messageType, string subscriber, out QueueInfo info)
        {
            var typeName = GetTypeName(messageType);
            var key = GetSubscriberName(typeName, ExchangeType.Fanout, subscriber);
            if (!_metaInfo.TryGetValue(key, out info))
            {
                info = new QueueInfo { Exchange = GetExchangeName(typeName, ExchangeType.Fanout), Queue = key };
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Fanout, durable: true, autoDelete: false);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
                channel.QueueBind(info.Queue, info.Exchange, string.Empty);

                _metaInfo.Set(key, info, _defaultExpiry);
            }
        }

        private string GetExchangeName(string messageType, string exchangeType)
        {
            return $"{messageType}.{exchangeType[0]}ex";
        }

        private string GetDelayKey(string messageType, string exchangeType, int delay)
        {
            return $"{messageType}.{exchangeType[0]}ex.delay_{delay}";
        }

        private string GetSubscriberName(string messageType, string exchangeType, string subscriber)
        {
            return $"{messageType}.{exchangeType[0]}sub.{subscriber}";
        }

        // for test only
        internal MemoryCache GetMetaInfoCache()
        {
            return this._metaInfo;
        }

        private string GetTypeName(Type messageType)
        {
            return messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name : messageType.Name;
        }
        #endregion
    }
}
