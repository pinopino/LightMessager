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
        public void RegisterHandler<TBody, THandler>(bool asyncConsumer = false)
            where THandler : BaseMessageHandler<TBody>
        {
            var handler = Activator.CreateInstance(typeof(THandler)) as THandler;
            IModel channel;
            IBasicConsumer consumer;
            if (!asyncConsumer)
            {
                channel = _connection.CreateModel();
                consumer = SetupConsumer<TBody, THandler>(channel, handler);
            }
            else
            {
                channel = _asynConnection.CreateModel();
                consumer = SetupAsyncConsumer<TBody, THandler>(channel, handler);
            }
            /*
              @param prefetchSize maximum amount of content (measured in octets) that the server will deliver, 0 if unlimited
              @param prefetchCount maximum number of messages that the server will deliver, 0 if unlimited
              @param global true if the settings should be applied to the entire channel rather than each consumer
            */
            channel.BasicQos(0, _prefetch_count, false);

            EnsureSendQueue(channel, typeof(TBody), out QueueInfo info);
            channel.BasicConsume(info.Queue, false, consumer);
        }

        public void RegisterHandler<TBody, THandler>(string subscriber, string[] subscribeKeys, bool asyncConsumer = false)
            where THandler : BaseMessageHandler<TBody>
        {
            var handler = Activator.CreateInstance(typeof(THandler)) as THandler;
            IModel channel;
            IBasicConsumer consumer;
            if (!asyncConsumer)
            {
                channel = _connection.CreateModel();
                consumer = SetupConsumer<TBody, THandler>(channel, handler);
            }
            else
            {
                channel = _asynConnection.CreateModel();
                consumer = SetupAsyncConsumer<TBody, THandler>(channel, handler);
            }
            channel.BasicQos(0, _prefetch_count, false);

            var useTopic = false;
            foreach (var key in subscribeKeys)
            {
                if (key.IndexOf('.') > 0)
                {
                    useTopic = true;
                    break;
                }
            }

            QueueInfo info;
            if (useTopic)
                EnsureTopicQueue(channel, typeof(TBody), subscriber, subscribeKeys, out info);
            else
                EnsureRouteQueue(channel, typeof(TBody), subscriber, subscribeKeys, out info);
            channel.BasicConsume(info.Queue, false, consumer);
        }

        public void RegisterHandler<TBody, THandler>(string subscriber, bool asyncConsumer = false)
            where THandler : BaseMessageHandler<TBody>
        {
            var handler = Activator.CreateInstance(typeof(THandler)) as THandler;
            IModel channel;
            IBasicConsumer consumer;
            if (!asyncConsumer)
            {
                channel = _connection.CreateModel();
                consumer = SetupConsumer<TBody, THandler>(channel, handler);
            }
            else
            {
                channel = _asynConnection.CreateModel();
                consumer = SetupAsyncConsumer<TBody, THandler>(channel, handler);
            }
            channel.BasicQos(0, _prefetch_count, false);

            EnsurePublishQueue(channel, typeof(TBody), subscriber, out QueueInfo info);
            channel.BasicConsume(info.Queue, false, consumer);
        }

        private EventingBasicConsumer SetupConsumer<TBody, THandler>(IModel channel, THandler handler)
            where THandler : BaseMessageHandler<TBody>
        {
            // 说明：
            // （6.0之后）consumer interface implementations must deserialize or copy delivery payload before 
            // delivery handler method returns. Retaining a reference to the payload is not safe: 
            // the memory allocated for it can be deallocated at any moment after the handler returns.
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var json = Encoding.UTF8.GetString(ea.Body);
                var msg = JsonConvert.DeserializeObject<Message<TBody>>(json);
                if (handler.Handle(msg, ea.Redelivered))
                    channel.BasicAck(ea.DeliveryTag, false);
                else
                    channel.BasicNack(ea.DeliveryTag, false, msg.NeedRequeue);
            };

            return consumer;
        }

        private AsyncEventingBasicConsumer SetupAsyncConsumer<TBody, THandler>(IModel channel, THandler handler)
            where THandler : BaseMessageHandler<TBody>
        {
            // 说明：虽然是异步，但是per channel的回调执行顺序还是保证了的（跟同步情况一致）
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                var json = Encoding.UTF8.GetString(ea.Body);
                var msg = JsonConvert.DeserializeObject<Message<TBody>>(json);
                if (await handler.HandleAsync(msg, ea.Redelivered))
                    channel.BasicAck(ea.DeliveryTag, false);
                else
                    channel.BasicNack(ea.DeliveryTag, false, msg.NeedRequeue);
            };

            return consumer;
        }
    }
}
