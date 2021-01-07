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
        public void RegisterHandler<TMessage, THandler>(bool asyncConsumer = false)
            where THandler : BaseMessageHandler<TMessage>
            where TMessage : BaseMessage
        {
            var handler = Activator.CreateInstance(typeof(THandler), _tracker) as THandler;
            IModel channel = null;
            IBasicConsumer consumer = null;
            if (!asyncConsumer)
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

        public void RegisterHandler<TMessage, THandler>(string subscriber, string[] subscribeKeys, bool asyncConsumer = false)
            where THandler : BaseMessageHandler<TMessage>
            where TMessage : BaseMessage
        {
            var handler = Activator.CreateInstance(typeof(THandler), _tracker) as THandler;
            IModel channel = null;
            IBasicConsumer consumer = null;
            if (!asyncConsumer)
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

        public void RegisterHandler<TMessage, THandler>(string subscriber, bool asyncConsumer = false)
            where THandler : BaseMessageHandler<TMessage>
            where TMessage : BaseMessage
        {
            var handler = Activator.CreateInstance(typeof(THandler), _tracker) as THandler;
            IModel channel = null;
            IBasicConsumer consumer = null;
            if (!asyncConsumer)
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
                if (ea.Redelivered && _tracker.GetMessage(ea.BasicProperties.MessageId) != null)
                {
                    // 之前一定有处理过
                    if (!handler.Idempotent)
                    {
                        channel.BasicNack(ea.DeliveryTag, false, false);
                        return;
                    }
                }

                // 两种情况可以断定没有处理过该条消息：
                // 1. Redelivered = false
                // 2. Redelivered = true && track = null
                // 需要说明的是在当前库实现中track=null是不可能的，track被当作单点永久存储在使用，所有消息发送
                // 之前一定会进行登记；
                // 最后，如果handler是幂等的那么也会走到这里来
                var json = Encoding.UTF8.GetString(ea.Body);
                var msg = JsonConvert.DeserializeObject<TMessage>(json);
                if (handler.Handle(msg))
                    channel.BasicAck(ea.DeliveryTag, false);
                else
                    channel.BasicNack(ea.DeliveryTag, false, msg.NeedRequeue);
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
                if (await handler.HandleAsync(msg))
                    channel.BasicAck(ea.DeliveryTag, false);
                else
                    channel.BasicNack(ea.DeliveryTag, false, msg.NeedRequeue);
            };

            return consumer;
        }
    }
}
