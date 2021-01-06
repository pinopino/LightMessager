using LightMessager.Model;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace LightMessager
{
    public sealed partial class RabbitMqHub
    {
        public void RegisterHandler<TMessage, THandler>(bool asyncConsume = false)
            where THandler : BaseMessageHandler<TMessage>
            where TMessage : BaseMessage
        {
            var handler = Activator.CreateInstance(typeof(THandler), _tracker) as THandler;
            IModel channel = null;
            IBasicConsumer consumer = null;
            if (!asyncConsume)
            {
                channel = _connection.CreateModel();
                consumer = SetupConsumer<TMessage, THandler>(channel, handler);
            }
            else
            {
                channel = _asynConnection.CreateModel();
                consumer = SetupAsyncConsumer<TMessage, THandler>(channel, handler);
            }
            /*
              @param prefetchSize maximum amount of content (measured in octets) that the server will deliver, 0 if unlimited
              @param prefetchCount maximum number of messages that the server will deliver, 0 if unlimited
              @param global true if the settings should be applied to the entire channel rather than each consumer
            */
            channel.BasicQos(0, _prefetch_count, false);

            EnsureSendQueue(channel, typeof(TMessage), out QueueInfo info);
            channel.BasicConsume(info.Queue, false, consumer);
        }

        public void RegisterHandler<TMessage, THandler>(string subscriber, string[] subscribeKeys, bool asyncConsume = false)
            where THandler : BaseMessageHandler<TMessage>
            where TMessage : BaseMessage
        {
            var handler = Activator.CreateInstance(typeof(THandler), _tracker) as THandler;
            IModel channel = null;
            IBasicConsumer consumer = null;
            if (!asyncConsume)
            {
                channel = _connection.CreateModel();
                consumer = SetupConsumer<TMessage, THandler>(channel, handler);
            }
            else
            {
                channel = _asynConnection.CreateModel();
                consumer = SetupAsyncConsumer<TMessage, THandler>(channel, handler);
            }
            channel.BasicQos(0, _prefetch_count, false);

            EnsureRouteQueue(channel, typeof(TMessage), subscriber, subscribeKeys, out QueueInfo info);
            channel.BasicConsume(info.Queue, false, consumer);
        }

        public void RegisterHandler<TMessage, THandler>(string subscriber, bool asyncConsume = false)
            where THandler : BaseMessageHandler<TMessage>
            where TMessage : BaseMessage
        {
            var handler = Activator.CreateInstance(typeof(THandler), _tracker) as THandler;
            IModel channel = null;
            IBasicConsumer consumer = null;
            if (!asyncConsume)
            {
                channel = _connection.CreateModel();
                consumer = SetupConsumer<TMessage, THandler>(channel, handler);
            }
            else
            {
                channel = _asynConnection.CreateModel();
                consumer = SetupAsyncConsumer<TMessage, THandler>(channel, handler);
            }
            channel.BasicQos(0, _prefetch_count, false);

            EnsurePublishQueue(channel, typeof(TMessage), subscriber, out QueueInfo info);
            channel.BasicConsume(info.Queue, false, consumer);
        }

        private EventingBasicConsumer SetupConsumer<TMessage, THandler>(IModel channel, THandler handler)
            where THandler : BaseMessageHandler<TMessage>
            where TMessage : BaseMessage
        {
            // 说明：
            // （6.0之后）consumer interface implementations must deserialize or copy delivery payload before 
            // delivery handler method returns. Retaining a reference to the payload is not safe: 
            // the memory allocated for it can be deallocated at any moment after the handler returns.
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                // 我们需要这样子来利用短路，大多数情况下耗时的GetModel都不会被调用到
                // TODO：如果右侧换成msg.Requeue>0？
                if (ea.Redelivered && _tracker.GetMessage(ea.BasicProperties.MessageId) != null)
                {
                    // 之前一定有处理过
                    if (!handler.Idempotent)
                    {
                        channel.BasicNack(ea.DeliveryTag, false, false);
                        return;
                    }
                }

                // 之前一定没有处理过（注意Redelivered为true但track=null也会落到这里来）；
                // 当然，如果handler是幂等的那么也会走到这里来
                var json = Encoding.UTF8.GetString(ea.Body);
                var msg = JsonConvert.DeserializeObject<TMessage>(json);
                handler.Handle(msg);

                if (msg.NeedRequeue)
                {
                    channel.BasicNack(ea.DeliveryTag, false, true);
                }
                else
                {
                    if (handler.Counter % _prefetch_count == 0)
                    {
                        channel.BasicAck(ea.DeliveryTag, true);
                    }
                }
            };

            return consumer;
        }

        private AsyncEventingBasicConsumer SetupAsyncConsumer<TMessage, THandler>(IModel channel, THandler handler)
            where THandler : BaseMessageHandler<TMessage>
            where TMessage : BaseMessage
        {
            // 说明：虽然是异步，但是per channel的回调执行顺序还是保证了的（跟同步情况是一样的）
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                if (ea.Redelivered && _tracker.GetMessage(ea.BasicProperties.MessageId) != null)
                {
                    if (!handler.Idempotent)
                    {
                        channel.BasicNack(ea.DeliveryTag, false, false);
                        return;
                    }
                }

                var json = Encoding.UTF8.GetString(ea.Body);
                var msg = JsonConvert.DeserializeObject<TMessage>(json);
                await handler.HandleAsync(msg);

                if (msg.NeedRequeue)
                {
                    channel.BasicNack(ea.DeliveryTag, false, true);
                }
                else
                {
                    if (handler.Counter % _prefetch_count == 0)
                    {
                        channel.BasicAck(ea.DeliveryTag, true);
                    }
                }
            };

            return consumer;
        }
    }
}
