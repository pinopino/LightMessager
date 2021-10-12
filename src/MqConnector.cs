﻿using LightMessager.Common;
using LightMessager.Model;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LightMessager
{
    internal sealed class MqConnector<TBody> : IDisposable
    {
        private static ObjectPool<IPooledWapper> _channel_pools;

        private bool _disposed;
        private int _batch_size;
        private ushort _prefetch_count;
        private IConnection _connection;
        private IConnection _asynConnection;

        public bool IsDisposed { get { return _disposed; } }

        public MqConnector(string host)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();
            InitConnection(configuration);
            InitAsyncConnection(configuration);
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
            var cpu = Environment.ProcessorCount;
            _channel_pools = new ObjectPool<IPooledWapper>(
                p => new PooledChannel(_connection.CreateModel(), p, _connection, false),
                cpu, cpu * 2);
        }

        private void InitOther(IConfigurationRoot configuration)
        {
            _batch_size = 300;
            _prefetch_count = 200;
        }

        public bool Send(TBody messageBody, string exchange, string routeKey)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MqConnector<TBody>));

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                var message = new Message<TBody>(messageBody);
                pooled.Publish(message, exchange, routeKey);

                return pooled.WaitForConfirms();
            }
        }

        public async ValueTask<bool> SendAsync(IEnumerable<TBody> messageBodys, string exchange, string routeKey)
        {
            if (!messageBodys.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                var counter = 0;
                var sequence = 0ul;
                foreach (var messageBody in messageBodys)
                {
                    var message = new Message<TBody>(messageBody);
                    sequence = await pooled.PublishReturnSeqAsync(message, exchange, routeKey);

                    if (++counter % _batch_size == 0)
                        await pooled.WaitForConfirmsAsync(sequence);
                }
                return await pooled.WaitForConfirmsAsync(sequence);
            }
        }

        public void Consume(string queue, Action<DeliverInfo> handler, bool autoAck = false)
        {
            if (string.IsNullOrEmpty(queue))
                throw new ArgumentNullException(nameof(queue));

            if (handler == null)
                throw new ArgumentNullException(nameof(handler));

            IModel channel = _connection.CreateModel();
            IBasicConsumer consumer = SetupConsumer(channel, (model, ea) =>
            {
                var info = new DeliverInfo();
                info.Body = new byte[ea.Body.Length];
                Buffer.BlockCopy(ea.Body, 0, info.Body, 0, ea.Body.Length);
                info.DeliveryTag = ea.DeliveryTag;
                info.Redelivered = ea.Redelivered;
                handler(info);
            });
            channel.BasicQos(0, _prefetch_count, false);
            channel.BasicConsume(queue, autoAck, consumer);
        }

        public void Consume(string queue, Func<DeliverInfo, Task> handler, bool autoAck = false)
        {
            if (string.IsNullOrEmpty(queue))
                throw new ArgumentNullException(nameof(queue));

            if (handler == null)
                throw new ArgumentNullException(nameof(handler));

            IModel channel = _asynConnection.CreateModel();
            IBasicConsumer consumer = SetupAsyncConsumer(channel, async (model, ea) =>
            {
                var info = new DeliverInfo();
                info.Body = new byte[ea.Body.Length];
                Buffer.BlockCopy(ea.Body, 0, info.Body, 0, ea.Body.Length);
                info.DeliveryTag = ea.DeliveryTag;
                info.Redelivered = ea.Redelivered;
                await handler(info);
            });
            channel.BasicQos(0, _prefetch_count, false);
            channel.BasicConsume(queue, autoAck, consumer);
        }

        private EventingBasicConsumer SetupConsumer(IModel channel, EventHandler<BasicDeliverEventArgs> handler)
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += handler;

            return consumer;
        }

        private AsyncEventingBasicConsumer SetupAsyncConsumer(IModel channel, AsyncEventHandler<BasicDeliverEventArgs> handler)
        {
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += handler;

            return consumer;
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
                _channel_pools.Dispose();
            }

            // 清理非托管资源

            // 让类型知道自己已经被释放
            _disposed = true;
        }
    }
}
