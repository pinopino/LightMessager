using LightMessager.Common;
using LightMessager.Model;
using LightMessager.Track;
using Microsoft.Extensions.Configuration;
using NLog;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace LightMessager
{
    /* 
     * links: 
     * https://www.rabbitmq.com/dotnet-api-guide.html
     * https://www.rabbitmq.com/queues.html
     * https://www.rabbitmq.com/channels.html
     * https://www.rabbitmq.com/reliability.html
     * https://www.rabbitmq.com/confirms.html
     * https://stackoverflow.com/questions/4444208/delayed-message-in-rabbitmq
    */
    public sealed partial class RabbitMqHub
    {
        private int _max_requeue;
        private int _max_republish;
        private int _min_delaysend;
        private ushort _prefetch_count;
        private int _batch_size;
        private BaseMessageTracker _tracker;
        private IConnection _connection;
        private IConnection _asynConnection;
        private static ObjectPool<IPooledWapper> _channel_pools;
        private ConcurrentDictionary<string, QueueInfo> _send_queue;
        private ConcurrentDictionary<string, QueueInfo> _route_queue;
        private ConcurrentDictionary<string, bool> _send_dlx;
        private ConcurrentDictionary<string, bool> _route_dlx;
        private object _lockobj = new object();
        private Logger _logger = LogManager.GetLogger("RabbitMqHub");

        public RabbitMqHub()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();
            InitConnection(configuration);
            InitAsyncConnection(configuration);
            InitMessageTracker(configuration);
            InitChannelPool(configuration);
            InitOther(configuration);
        }

        public RabbitMqHub(IConfigurationRoot configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            InitConnection(configuration);
            InitAsyncConnection(configuration);
            InitMessageTracker(configuration);
            InitChannelPool(configuration);
            InitOther(configuration);
        }

        private void InitConnection(IConfigurationRoot configuration)
        {
            var factory = GetConnectionFactory(configuration);
            _connection = factory.CreateConnection();
        }

        private void InitAsyncConnection(IConfigurationRoot configuration)
        {
            var factory = GetConnectionFactory(configuration);
            factory.DispatchConsumersAsync = true;
            _asynConnection = factory.CreateConnection();
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

        private void InitChannelPool(IConfigurationRoot configuration)
        {
            // 说明：注意这里使用的是_connection，后面可以考虑producer的channel走
            // 自己的connection（甚至connection也可以池化掉）
            var cpu = Environment.ProcessorCount;
            _channel_pools = new ObjectPool<IPooledWapper>(
                p => new PooledChannel(_connection.CreateModel(), p, _tracker, _connection),
                cpu, cpu * 2);
        }

        private void InitMessageTracker(IConfigurationRoot configuration)
        {
            _tracker = new InMemoryTracker();
        }

        private void InitOther(IConfigurationRoot configuration)
        {
            _max_republish = 2;
            _max_requeue = 2;
            _min_delaysend = 5;
            _batch_size = 300;
            _prefetch_count = 200;
            _send_queue = new ConcurrentDictionary<string, QueueInfo>();
            _route_queue = new ConcurrentDictionary<string, QueueInfo>();
            _send_dlx = new ConcurrentDictionary<string, bool>();
            _route_dlx = new ConcurrentDictionary<string, bool>();
        }

        internal bool PreTrackMessage(BaseMessage message)
        {
            var model = _tracker.GetMessage(message.MsgId);
            if (model != null)
            {
                if (model.State == MessageState.Created && model.Republish < _max_republish)
                {
                    model.Republish += 1;
                    return true;
                }
            }
            else
            {
                return _tracker.AddMessage(message);
            }

            return false;
        }

        // send方式的生产端
        internal void EnsureSendQueue(IModel channel, Type messageType, int delaySend, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_send_queue.TryGetValue(key, out info))
            {
                info = GetSendQueueInfo(key);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
            }

            if (delaySend > 0)
            {
                // links:
                // https://www.rabbitmq.com/ttl.html
                // https://www.rabbitmq.com/dlx.html
                delaySend = Math.Max(delaySend, _min_delaysend); // 至少保证有个几秒的延时，不然意义不大
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_send_dlx.ContainsKey(dlx_key))
                {
                    info.Delay_Exchange = string.Empty;
                    info.Delay_Queue = dlx_key;

                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend * 1000);
                    args.Add("x-dead-letter-exchange", info.Delay_Exchange);
                    args.Add("x-dead-letter-routing-key", info.Queue);
                    channel.QueueDeclare(
                        info.Delay_Queue,
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: args);

                    _send_dlx.TryAdd(dlx_key, true);
                }
            }
        }

        // send方式的消费端
        internal void EnsureSendQueue(IModel channel, Type messageType, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_send_queue.TryGetValue(key, out info))
            {
                info = GetSendQueueInfo(key);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
            }
        }

        // send with route方式的生产端
        internal void EnsureRouteQueue(IModel channel, Type messageType, int delaySend, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_route_queue.TryGetValue(key, out info))
            {
                info = GetRouteQueueInfo(key);
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Direct, durable: true);
            }

            if (delaySend > 0)
            {
                delaySend = Math.Max(delaySend, _min_delaysend);
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_route_dlx.ContainsKey(dlx_key))
                {
                    info.Delay_Exchange = info.Exchange;
                    info.Delay_Queue = dlx_key;

                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend * 1000);
                    args.Add("x-dead-letter-exchange", info.Delay_Exchange);
                    channel.QueueDeclare(
                        info.Delay_Queue,
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: args);
                    channel.QueueBind(info.Delay_Queue, info.Delay_Exchange, string.Empty);

                    _route_dlx.TryAdd(key, true);
                }
            }
        }

        // send with route方式的消费端
        internal void EnsureRouteQueue(IModel channel, Type messageType, string subscriber, string[] subscribeKeys, out QueueInfo info)
        {
            var type_name = GetTypeName(messageType);
            var key = $"{type_name}.sub.{subscriber}";
            if (!_route_queue.TryGetValue(key, out info))
            {
                info = GetRouteQueueInfo(key, type_name);
                info.Queue = key;
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Direct, durable: true);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
                for (var i = 0; i < subscribeKeys.Length; i++)
                    channel.QueueBind(info.Queue, info.Exchange, routingKey: subscribeKeys[i]);
            }
        }

        // send with topic方式的生产端
        internal void EnsureTopicQueue(IModel channel, Type messageType, int delaySend, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_route_queue.TryGetValue(key, out info))
            {
                info = GetRouteQueueInfo(key);
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Topic, durable: true);
            }

            if (delaySend > 0)
            {
                delaySend = Math.Max(delaySend, _min_delaysend);
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_route_dlx.ContainsKey(dlx_key))
                {
                    info.Delay_Exchange = info.Exchange;
                    info.Delay_Queue = dlx_key;

                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend * 1000);
                    args.Add("x-dead-letter-exchange", info.Delay_Exchange);
                    channel.QueueDeclare(
                        info.Delay_Queue,
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: args);
                    channel.QueueBind(info.Delay_Queue, info.Delay_Exchange, string.Empty);

                    _route_dlx.TryAdd(key, true);
                }
            }
        }

        // send with topic方式的消费端
        internal void EnsureTopicQueue(IModel channel, Type messageType, string subscriber, string[] subscribeKeys, out QueueInfo info)
        {
            var type_name = GetTypeName(messageType);
            var key = $"{type_name}.sub.{subscriber}";
            if (!_route_queue.TryGetValue(key, out info))
            {
                info = GetRouteQueueInfo(key, type_name);
                info.Queue = key;
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Topic, durable: true);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
                for (var i = 0; i < subscribeKeys.Length; i++)
                    channel.QueueBind(info.Queue, info.Exchange, routingKey: subscribeKeys[i]);
            }
        }

        // publish（fanout）方式的生产端
        internal void EnsurePublishQueue(IModel channel, Type messageType, int delaySend, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_route_queue.TryGetValue(key, out info))
            {
                info = GetPublishQueueInfo(key);
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Fanout, durable: true);
            }

            if (delaySend > 0)
            {
                delaySend = Math.Max(delaySend, _min_delaysend);
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_route_dlx.ContainsKey(dlx_key))
                {
                    info.Delay_Exchange = info.Exchange;
                    info.Delay_Queue = dlx_key;

                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend * 1000);
                    args.Add("x-dead-letter-exchange", info.Delay_Exchange);
                    channel.QueueDeclare(
                        info.Delay_Queue,
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: args);
                    channel.QueueBind(info.Delay_Queue, info.Delay_Exchange, string.Empty);

                    _route_dlx.TryAdd(key, true);
                }
            }
        }

        // publish（fanout）方式的消费端
        internal void EnsurePublishQueue(IModel channel, Type messageType, string subscriber, out QueueInfo info)
        {
            var type_name = GetTypeName(messageType);
            var key = $"{type_name}.sub.{subscriber}";
            if (!_route_queue.TryGetValue(key, out info))
            {
                info = GetPublishQueueInfo(key, type_name);
                info.Queue = key;
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Fanout, durable: true);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
                channel.QueueBind(info.Queue, info.Exchange, string.Empty);
            }
        }

        internal QueueInfo GetSendQueueInfo(string key)
        {
            var info = _send_queue.GetOrAdd(key, t => new QueueInfo
            {
                Exchange = string.Empty,
                Queue = key
            });

            return info;
        }

        internal QueueInfo GetRouteQueueInfo(string key, string typeName = null)
        {
            var info = _route_queue.GetOrAdd(key, t => new QueueInfo
            {
                Exchange = (typeName ?? key) + ".ex.direct",
                Queue = string.Empty
            });

            return info;
        }

        internal QueueInfo GetTopicQueueInfo(string key, string typeName = null)
        {
            var info = _route_queue.GetOrAdd(key, t => new QueueInfo
            {
                Exchange = (typeName ?? key) + ".ex.topic",
                Queue = string.Empty
            });

            return info;
        }

        internal QueueInfo GetPublishQueueInfo(string key, string typeName = null)
        {
            var info = _route_queue.GetOrAdd(key, t => new QueueInfo
            {
                Exchange = (typeName ?? key) + ".ex.fanout",
                Queue = string.Empty
            });

            return info;
        }

        private string GetTypeName(Type messageType)
        {
            return messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name : messageType.Name;
        }
    }
}
