using LightMessager.Common;
using LightMessager.Model;
using LightMessager.Track;
using Microsoft.Extensions.Configuration;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

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
        public class Advance
        {
            private ushort _prefetch_count;
            private IConnection _connection;
            private IConnection _asynConnection;
            public Advance(IConnection connection, IConnection asynConnection, ushort prefetchCount)
            {
                _connection = connection;
                _asynConnection = asynConnection;
                _prefetch_count = prefetchCount;
            }

            public void Send(object message, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers = null)
            {
                using (var channel = _connection.CreateModel())
                {
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    properties.Headers = headers;
                    properties.ContentType = "text/plain";
                    properties.DeliveryMode = 2;

                    var json = Newtonsoft.Json.JsonConvert.SerializeObject(message);
                    var bytes = System.Text.Encoding.UTF8.GetBytes(json);
                    channel.BasicPublish(exchange,
                                         routeKey,
                                         mandatory,
                                         properties,
                                         bytes);
                }
            }

            public void Consume(string queue, Action<object, BasicDeliverEventArgs> action)
            {
                var channel = _connection.CreateModel();
                channel.BasicQos(0, _prefetch_count, false);
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => action(model, ea);
                channel.BasicConsume(queue, false, consumer);
            }

            public void Consume(string queue, Func<object, BasicDeliverEventArgs, Task> func)
            {
                var channel = _asynConnection.CreateModel();
                channel.BasicQos(0, _prefetch_count, false);
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.Received += async (model, ea) => await func(model, ea);
                channel.BasicConsume(queue, false, consumer);
            }

            public void ExchangeDeclare(string exchange, string type, bool durable = true, IDictionary<string, object> arguments = null)
            {
                using (var channel = _connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange, type, durable, autoDelete: false, arguments);
                }
            }

            public void QueueDeclare(string queue, bool durable = true, IDictionary<string, object> arguments = null)
            {
                using (var channel = _connection.CreateModel())
                {
                    channel.QueueDeclare(queue, durable, exclusive: false, autoDelete: false, arguments);
                }
            }

            public void QueueBind(string queue, string exchange, string routeKey, IDictionary<string, object> arguments = null)
            {
                using (var channel = _connection.CreateModel())
                {
                    channel.QueueBind(queue, exchange, routeKey, arguments);
                }
            }
        }

        internal Lazy<ObjectPool<IPooledWapper>> _channel_pools;
        internal ObjectPool<IPooledWapper> _confirmed_channel_pools;
        private int _max_requeue;
        private int _max_republish;
        private int _min_delaysend;
        private int _batch_size;
        private ushort _prefetch_count;
        internal bool _publish_confirm;
        internal TimeSpan? _confirm_timeout;
        internal IConnection _connection;
        internal IConnection _asynConnection;
        private ConcurrentDictionary<string, QueueInfo> _send_queue;
        private ConcurrentDictionary<string, QueueInfo> _route_queue;
        private ConcurrentDictionary<string, bool> _send_dlx;
        private ConcurrentDictionary<string, bool> _route_dlx;
        internal IMessageSendTracker _send_tracker;
        internal IMessageRecvTracker _recv_tracker;
        private object _lockobj = new object();
        private Logger _logger = LogManager.GetLogger("RabbitMqHub");

        public Advance Advanced { private set; get; }

        public RabbitMqHub()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();
            InitConnection(configuration);
            InitAsyncConnection(configuration);
            InitChannelPool();
            InitOther(configuration);
            InitAdvance();
        }

        public RabbitMqHub(IConfigurationRoot configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            InitConnection(configuration);
            InitAsyncConnection(configuration);
            InitChannelPool();
            InitOther(configuration);
            InitAdvance();
        }

        public RabbitMqHub SetPublishConfirm(TimeSpan timeout = default)
        {
            _publish_confirm = true;
            if (timeout != default)
                _confirm_timeout = timeout;

            InitConfirmedChannelPool();

            return this;
        }

        public RabbitMqHub SetSendTracker(IMessageSendTracker sendTracker)
        {
            _send_tracker = sendTracker;
            return this;
        }

        public RabbitMqHub SetRecvTracker(IMessageRecvTracker recvTracker)
        {
            _recv_tracker = recvTracker;
            return this;
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

        private void InitChannelPool()
        {
            var cpu = Environment.ProcessorCount;
            _channel_pools = new Lazy<ObjectPool<IPooledWapper>>(() => new ObjectPool<IPooledWapper>(p => new PooledChannel(this), 4, cpu));
        }

        private void InitConfirmedChannelPool()
        {
            var cpu = Environment.ProcessorCount;
            _confirmed_channel_pools = new ObjectPool<IPooledWapper>(p => new PooledChannel(this, publishConfirm: true), 4, cpu);
        }

        private void InitAdvance()
        {
            Advanced = new Advance(_connection, _asynConnection, _prefetch_count);
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

        internal IPooledWapper GetChannel()
        {
            var pool = _publish_confirm ? _confirmed_channel_pools : _channel_pools.Value;
            return pool.Get();
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
                    info.Delay_Exchange = $"{key}.ex.delay_{delaySend}";
                    info.Delay_Queue = dlx_key;

                    channel.ExchangeDeclare(info.Delay_Exchange, ExchangeType.Fanout, durable: true);
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend * 1000);
                    args.Add("x-dead-letter-exchange", info.Exchange);
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
                info = GetTopicQueueInfo(key);
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Topic, durable: true);
            }

            if (delaySend > 0)
            {
                delaySend = Math.Max(delaySend, _min_delaysend);
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_route_dlx.ContainsKey(dlx_key))
                {
                    info.Delay_Exchange = $"{key}.ex.delay_{delaySend}";
                    info.Delay_Queue = dlx_key;

                    channel.ExchangeDeclare(info.Delay_Exchange, ExchangeType.Fanout, durable: true);
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend * 1000);
                    args.Add("x-dead-letter-exchange", info.Exchange);
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
                info = GetTopicQueueInfo(key, type_name);
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
                    info.Delay_Exchange = $"{key}.ex.delay_{delaySend}";
                    info.Delay_Queue = dlx_key;

                    channel.ExchangeDeclare(info.Delay_Exchange, ExchangeType.Fanout, durable: true);
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend * 1000);
                    args.Add("x-dead-letter-exchange", info.Exchange);
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
