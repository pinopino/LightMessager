using LightMessager.Common;
using LightMessager.Exceptions;
using LightMessager.Model;
using Microsoft.Extensions.Configuration;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
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
            private RabbitMqHub _rabbitMqHub;
            public Advance(RabbitMqHub rabbitMqHub)
            {
                _rabbitMqHub = rabbitMqHub;
            }

            public void Send<TBody>(TBody messageBody, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers = null)
            {
                using (var pooled = _rabbitMqHub.GetChannel() as PooledChannel)
                {
                    var message = new Message<TBody>(messageBody);
                    pooled.Publish(message, exchange, routeKey, mandatory, headers);
                }
            }

            public async Task SendAsync<TBody>(TBody messageBody, string exchange, string routeKey, bool mandatory, IDictionary<string, object> headers = null)
            {
                using (var pooled = _rabbitMqHub.GetChannel() as PooledChannel)
                {
                    var message = new Message<TBody>(messageBody);
                    await pooled.PublishAsync(message, exchange, routeKey, mandatory, headers);
                }
            }

            public void Consume(string queue, Action<object, BasicDeliverEventArgs> action, bool requeue = false)
            {
                var channel = _rabbitMqHub.connection.CreateModel();
                channel.BasicQos(0, _rabbitMqHub._prefetchCount, false);
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var msgId = ea.BasicProperties.MessageId;
                    try
                    {
                        var json = Encoding.UTF8.GetString(ea.Body);
                        _rabbitMqHub.OnMessageReceived(msgId, json);
                        action(model, ea);
                        _rabbitMqHub.OnMessageConsumeOK(msgId);
                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch (LightMessagerException ex)
                    {
                        _rabbitMqHub.OnMessageConsumeFailed(msgId, ex.Message);
                        channel.BasicNack(ea.DeliveryTag, false, requeue);
                    }
                    catch (Exception ex)
                    {
                        _rabbitMqHub.OnMessageConsumeFailed(msgId, ex.Message);
                    }
                };
                channel.BasicConsume(queue, false, consumer);
            }

            public void Consume(string queue, Func<object, BasicDeliverEventArgs, Task> func, bool requeue = false)
            {
                var channel = _rabbitMqHub.asynConnection.CreateModel();
                channel.BasicQos(0, _rabbitMqHub._prefetchCount, false);
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.Received += async (model, ea) =>
                {
                    var msgId = ea.BasicProperties.MessageId;
                    try
                    {
                        var json = Encoding.UTF8.GetString(ea.Body);
                        _rabbitMqHub.OnMessageReceived(msgId, json);
                        await func(model, ea);
                        _rabbitMqHub.OnMessageConsumeOK(msgId);
                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch (Exception ex)
                    {
                        _rabbitMqHub.OnMessageConsumeFailed(msgId, ex.Message);
                        channel.BasicNack(ea.DeliveryTag, false, requeue);
                    }
                };
                channel.BasicConsume(queue, false, consumer);
            }

            public void ExchangeDeclare(string exchange, string type, bool durable = true, IDictionary<string, object> arguments = null, IModel channel = null)
            {
                if (channel != null)
                {
                    channel.ExchangeDeclare(exchange, type, durable, autoDelete: false, arguments);
                }
                else
                {
                    using (var created = _rabbitMqHub.connection.CreateModel())
                    {
                        created.ExchangeDeclare(exchange, type, durable, autoDelete: false, arguments);
                    }
                }
            }

            public void QueueDeclare(string queue, bool durable = true, IDictionary<string, object> arguments = null, IModel channel = null)
            {
                if (channel != null)
                {
                    channel.QueueDeclare(queue, durable, exclusive: false, autoDelete: false, arguments);
                }
                else
                {
                    using (var created = _rabbitMqHub.connection.CreateModel())
                    {
                        created.QueueDeclare(queue, durable, exclusive: false, autoDelete: false, arguments);
                    }
                }
            }

            public void QueueBind(string queue, string exchange, string routeKey, IDictionary<string, object> arguments = null, IModel channel = null)
            {
                if (channel != null)
                {
                    channel.QueueBind(queue, exchange, routeKey, arguments);
                }
                else
                {
                    using (var created = _rabbitMqHub.connection.CreateModel())
                    {
                        created.QueueBind(queue, exchange, routeKey, arguments);
                    }
                }
            }
        }

        private Logger _logger = LogManager.GetLogger("RabbitMqHub");

        private int _maxRequeue;
        private int _maxRepublish;
        private int _minDelaySend;
        private int _batchSize;
        private ushort _prefetchCount;
        private ConcurrentDictionary<string, QueueInfo> _sendQueue;
        private ConcurrentDictionary<string, QueueInfo> _routeQueue;
        private ConcurrentDictionary<string, bool> _sendDlx;
        private ConcurrentDictionary<string, bool> _routeDlx;
        private object _lockObj = new object();

        internal Lazy<ObjectPool<IPooledWapper>> channelPools;
        internal ObjectPool<IPooledWapper> confirmedChannelPools;
        internal bool publishConfirm;
        internal TimeSpan? confirmTimeout;
        internal IConnection connection;
        internal IConnection asynConnection;

        public Advance Advanced { private set; get; }
        // TODO：async event
        //
        // links：
        // https://stackoverflow.com/questions/19415646/should-i-avoid-async-void-event-handlers
        // https://stackoverflow.com/questions/12451609/how-to-await-raising-an-eventhandler-event
        // send
        public event EventHandler<MessageSendEventArgs> MessageSending;
        public event EventHandler<MessageSendEventArgs> MessageSendOK;
        public event EventHandler<MessageSendEventArgs> MessageSendFailed;
        // recv/consume
        public event EventHandler<MessageConsumeEventArgs> MessageReceived;
        public event EventHandler<MessageConsumeEventArgs> MessageConsumeOK;
        public event EventHandler<MessageConsumeEventArgs> MessageConsumeFailed;

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
            publishConfirm = true;
            if (timeout != default)
                confirmTimeout = timeout;

            InitConfirmedChannelPool();

            return this;
        }

        internal void OnMessageSending(ulong nextPublishSeqNo, in Message message)
        {
            // Make a temporary copy of the event to avoid possibility of
            // a race condition if the last subscriber unsubscribes
            // immediately after the null check and before the event is raised.
            // 下同。
            var raiseEvent = MessageSending;
            raiseEvent?.Invoke(null, new MessageSendEventArgs
            {
                DeliveryTag = nextPublishSeqNo,
                SendStatus = SendStatus.PendingResponse,
                Message = message
            });
        }

        internal void OnMessageSend(in Message message, SendStatus newStatus, string remark = "")
        {
            var raiseEvent = MessageSendFailed;
            raiseEvent?.Invoke(null, new MessageSendEventArgs
            {
                SendStatus = newStatus,
                Message = message,
                Remark = remark
            });
        }

        internal void OnChannelBasicAcks(object sender, BasicAckEventArgs e)
        {
            var raiseEvent = MessageSendOK;
            raiseEvent?.Invoke(null, new MessageSendEventArgs
            {
                SendStatus = SendStatus.Confirmed,
                DeliveryTag = e.DeliveryTag,
                Multiple = e.Multiple
            });
        }

        internal void OnChannelBasicNacks(object sender, BasicNackEventArgs e)
        {
            var raiseEvent = MessageSendFailed;
            raiseEvent?.Invoke(null, new MessageSendEventArgs
            {
                SendStatus = SendStatus.Nacked,
                DeliveryTag = e.DeliveryTag,
                Multiple = e.Multiple
            });
        }

        internal void OnChannelBasicReturn(object sender, BasicReturnEventArgs e)
        {
            var raiseEvent = MessageSendFailed;
            raiseEvent?.Invoke(null, new MessageSendEventArgs
            {
                SendStatus = SendStatus.Unroutable,
                ReplyCode = e.ReplyCode,
                ReplyText = e.ReplyText,
                Remark = $"try to push to exchange[{e.Exchange}] with routekey[{e.RoutingKey}]"
            });
        }

        internal void OnMessageReceived(string msgId, string msgJson)
        {
            var raiseEvent = MessageReceived;
            raiseEvent?.Invoke(null, new MessageConsumeEventArgs
            {
                ConsumeStatus = ConsumeStatus.Received,
                MessageId = msgId,
                MessageJson = msgJson
            });
        }

        internal void OnMessageConsumeOK(string msgId)
        {
            var raiseEvent = MessageConsumeOK;
            raiseEvent?.Invoke(null, new MessageConsumeEventArgs
            {
                ConsumeStatus = ConsumeStatus.Consumed,
                MessageId = msgId
            });
        }

        internal void OnMessageConsumeFailed(string msgId, string remark = "")
        {
            var raiseEvent = MessageConsumeFailed;
            raiseEvent?.Invoke(null, new MessageConsumeEventArgs
            {
                ConsumeStatus = ConsumeStatus.Failed,
                MessageId = msgId,
                Remark = remark
            });
        }

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
            channelPools = new Lazy<ObjectPool<IPooledWapper>>(() => new ObjectPool<IPooledWapper>(p => new PooledChannel(this), min, cpu));
        }

        private void InitConfirmedChannelPool()
        {
            var min = 4;
            var cpu = Math.Max(Environment.ProcessorCount, min);
            confirmedChannelPools = new ObjectPool<IPooledWapper>(p => new PooledConfirmedChannel(this), min, cpu);
        }

        private void InitAdvance()
        {
            Advanced = new Advance(this);
        }

        private void InitOther(IConfigurationRoot configuration)
        {
            _maxRepublish = 2;
            _maxRequeue = 2;
            _minDelaySend = 5;
            _batchSize = 300;
            _prefetchCount = 200;
            _sendQueue = new ConcurrentDictionary<string, QueueInfo>();
            _routeQueue = new ConcurrentDictionary<string, QueueInfo>();
            _sendDlx = new ConcurrentDictionary<string, bool>();
            _routeDlx = new ConcurrentDictionary<string, bool>();
        }

        internal IPooledWapper GetChannel()
        {
            var pool = publishConfirm ? confirmedChannelPools : channelPools.Value;
            return pool.Get();
        }

        // send方式的生产端
        internal void EnsureSendQueue(IModel channel, Type messageType, int delaySend, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_sendQueue.TryGetValue(key, out info))
            {
                info = GetSendQueueInfo(key);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
            }

            if (delaySend > 0)
            {
                // links:
                // https://www.rabbitmq.com/ttl.html
                // https://www.rabbitmq.com/dlx.html
                delaySend = Math.Max(delaySend, _minDelaySend); // 至少保证有个几秒的延时，不然意义不大
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_sendDlx.ContainsKey(dlx_key))
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

                    _sendDlx.TryAdd(dlx_key, true);
                }
            }
        }

        // send方式的消费端
        internal void EnsureSendQueue(IModel channel, Type messageType, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_sendQueue.TryGetValue(key, out info))
            {
                info = GetSendQueueInfo(key);
                channel.QueueDeclare(info.Queue, durable: true, exclusive: false, autoDelete: false);
            }
        }

        // send with route方式的生产端
        internal void EnsureRouteQueue(IModel channel, Type messageType, int delaySend, out QueueInfo info)
        {
            var key = GetTypeName(messageType);
            if (!_routeQueue.TryGetValue(key, out info))
            {
                info = GetRouteQueueInfo(key);
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Direct, durable: true);
            }

            if (delaySend > 0)
            {
                delaySend = Math.Max(delaySend, _minDelaySend);
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_routeDlx.ContainsKey(dlx_key))
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

                    _routeDlx.TryAdd(key, true);
                }
            }
        }

        // send with route方式的消费端
        internal void EnsureRouteQueue(IModel channel, Type messageType, string subscriber, string[] subscribeKeys, out QueueInfo info)
        {
            var type_name = GetTypeName(messageType);
            var key = $"{type_name}.sub.{subscriber}";
            if (!_routeQueue.TryGetValue(key, out info))
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
            if (!_routeQueue.TryGetValue(key, out info))
            {
                info = GetTopicQueueInfo(key);
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Topic, durable: true);
            }

            if (delaySend > 0)
            {
                delaySend = Math.Max(delaySend, _minDelaySend);
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_routeDlx.ContainsKey(dlx_key))
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

                    _routeDlx.TryAdd(key, true);
                }
            }
        }

        // send with topic方式的消费端
        internal void EnsureTopicQueue(IModel channel, Type messageType, string subscriber, string[] subscribeKeys, out QueueInfo info)
        {
            var type_name = GetTypeName(messageType);
            var key = $"{type_name}.sub.{subscriber}";
            if (!_routeQueue.TryGetValue(key, out info))
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
            if (!_routeQueue.TryGetValue(key, out info))
            {
                info = GetPublishQueueInfo(key);
                channel.ExchangeDeclare(info.Exchange, ExchangeType.Fanout, durable: true);
            }

            if (delaySend > 0)
            {
                delaySend = Math.Max(delaySend, _minDelaySend);
                var dlx_key = $"{key}.delay_{delaySend}";
                if (!_routeDlx.ContainsKey(dlx_key))
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

                    _routeDlx.TryAdd(key, true);
                }
            }
        }

        // publish（fanout）方式的消费端
        internal void EnsurePublishQueue(IModel channel, Type messageType, string subscriber, out QueueInfo info)
        {
            var type_name = GetTypeName(messageType);
            var key = $"{type_name}.sub.{subscriber}";
            if (!_routeQueue.TryGetValue(key, out info))
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
            var info = _sendQueue.GetOrAdd(key, t => new QueueInfo
            {
                Exchange = string.Empty,
                Queue = key
            });

            return info;
        }

        internal QueueInfo GetRouteQueueInfo(string key, string typeName = null)
        {
            var info = _routeQueue.GetOrAdd(key, t => new QueueInfo
            {
                Exchange = (typeName ?? key) + ".ex.direct",
                Queue = string.Empty
            });

            return info;
        }

        internal QueueInfo GetTopicQueueInfo(string key, string typeName = null)
        {
            var info = _routeQueue.GetOrAdd(key, t => new QueueInfo
            {
                Exchange = (typeName ?? key) + ".ex.topic",
                Queue = string.Empty
            });

            return info;
        }

        internal QueueInfo GetPublishQueueInfo(string key, string typeName = null)
        {
            var info = _routeQueue.GetOrAdd(key, t => new QueueInfo
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
