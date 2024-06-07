using LightMessager.Model;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace LightMessager
{
    public sealed partial class RabbitMqHub
    {
        public class Advanced
        {
            private RabbitMqHub _rabbitMqHub;
            public Advanced(RabbitMqHub rabbitMqHub)
            {
                _rabbitMqHub = rabbitMqHub;
            }

            public IConnection Connection
            {
                get
                {
                    return _rabbitMqHub.connection;
                }
            }

            #region 发送
            public void Send(object message, string exchange, string routeKey, bool mandatory, bool publishConfirm = false, IDictionary<string, object> headers = null)
            {
                if (publishConfirm)
                {
                    using (var pooled = _rabbitMqHub.GetConfirmChannelFromPool() as PooledConfirmChannel)
                    {
                        pooled.Publish(message, exchange, routeKey, mandatory, headers);
                        pooled.WaitForConfirms();
                    }
                }
                else
                {
                    using (var pooled = _rabbitMqHub.GetChannelFromPool() as PooledChannel)
                        pooled.Publish(message, exchange, routeKey, mandatory, headers);
                }
            }

            public void Send(IEnumerable<object> messages, string exchange, string routeKey, bool mandatory, bool publishConfirm = false, IDictionary<string, object> headers = null)
            {
                if (publishConfirm)
                {
                    using (var pooled = _rabbitMqHub.GetConfirmChannelFromPool() as PooledConfirmChannel)
                    {
                        var counter = 0;
                        foreach (var message in messages)
                        {
                            pooled.Publish(message, exchange, routeKey, mandatory, headers);

                            if (++counter % _rabbitMqHub._batchSize == 0)
                                pooled.WaitForConfirms();
                        }
                        pooled.WaitForConfirms();
                    }
                }
                else
                {
                    using (var pooled = _rabbitMqHub.GetChannelFromPool() as PooledChannel)
                    {
                        foreach (var message in messages)
                            pooled.Publish(message, exchange, routeKey, mandatory, headers);
                    }
                }
            }

            public Task SendAsync(object message, string exchange, string routeKey, bool mandatory, bool publishConfirm = false, IDictionary<string, object> headers = null)
            {
                return Task.Factory.StartNew(() =>
                {
                    Send(message, exchange, routeKey, mandatory, publishConfirm, headers);
                });
            }

            public Task SendAsync(IEnumerable<object> messages, string exchange, string routeKey, bool mandatory, bool publishConfirm = false, IDictionary<string, object> headers = null)
            {
                return Task.Factory.StartNew(() =>
                {
                    Send(messages, exchange, routeKey, mandatory, publishConfirm, headers);
                });
            }
            #endregion

            #region 消费
            public void RegisterHandler<TMessage, THandler>(bool asyncConsumer = false)
             where THandler : BaseMessageHandler<TMessage>
            {
                var handler = Activator.CreateInstance(typeof(THandler)) as THandler;

                IModel channel;
                IBasicConsumer consumer;
                if (!asyncConsumer)
                {
                    channel = _rabbitMqHub.connection.CreateModel();
                    consumer = SetupConsumer<TMessage, THandler>(channel, handler);
                }
                else
                {
                    channel = _rabbitMqHub.asynConnection.CreateModel();
                    consumer = SetupAsyncConsumer<TMessage, THandler>(channel, handler);
                }
                /*
                  @param prefetchSize maximum amount of content (measured in octets) that the server will deliver, 0 if unlimited
                  @param prefetchCount maximum number of messages that the server will deliver, 0 if unlimited
                  @param global true if the settings should be applied to the entire channel rather than each consumer
                */
                channel.BasicQos(0, _rabbitMqHub._prefetchCount, false);

                _rabbitMqHub.EnsureSendQueue(channel, typeof(TMessage), out QueueInfo info);
                channel.BasicConsume(info.Queue, false, consumer);
            }

            public void RegisterHandler<TMessage, THandler>(string subscriber, string[] routeKeys, bool asyncConsumer = false)
                where THandler : BaseMessageHandler<TMessage>
            {
                var handler = Activator.CreateInstance(typeof(THandler)) as THandler;

                IModel channel;
                IBasicConsumer consumer;
                if (!asyncConsumer)
                {
                    channel = _rabbitMqHub.connection.CreateModel();
                    consumer = SetupConsumer<TMessage, THandler>(channel, handler);
                }
                else
                {
                    channel = _rabbitMqHub.asynConnection.CreateModel();
                    consumer = SetupAsyncConsumer<TMessage, THandler>(channel, handler);
                }
                channel.BasicQos(0, _rabbitMqHub._prefetchCount, false);

                var useTopic = false;
                foreach (var key in routeKeys)
                {
                    if (key.IndexOf('.') > 0)
                    {
                        useTopic = true;
                        break;
                    }
                }
                QueueInfo info;
                if (useTopic)
                    _rabbitMqHub.EnsureTopicQueue(channel, typeof(TMessage), subscriber, routeKeys, out info);
                else
                    _rabbitMqHub.EnsureRouteQueue(channel, typeof(TMessage), subscriber, routeKeys, out info);
                channel.BasicConsume(info.Queue, false, consumer);
            }

            public void RegisterHandler<TMessage, THandler>(string subscriber, bool asyncConsumer = false)
                where THandler : BaseMessageHandler<TMessage>
            {
                var handler = Activator.CreateInstance(typeof(THandler)) as THandler;

                IModel channel;
                IBasicConsumer consumer;
                if (!asyncConsumer)
                {
                    channel = _rabbitMqHub.connection.CreateModel();
                    consumer = SetupConsumer<TMessage, THandler>(channel, handler);
                }
                else
                {
                    channel = _rabbitMqHub.asynConnection.CreateModel();
                    consumer = SetupAsyncConsumer<TMessage, THandler>(channel, handler);
                }
                channel.BasicQos(0, _rabbitMqHub._prefetchCount, false);

                _rabbitMqHub.EnsurePublishQueue(channel, typeof(TMessage), subscriber, out QueueInfo info);
                channel.BasicConsume(info.Queue, false, consumer);
            }

            private EventingBasicConsumer SetupConsumer<TMessage, THandler>(IModel channel, THandler handler)
                where THandler : BaseMessageHandler<TMessage>
            {
                // 说明：
                // （6.0之后）consumer interface implementations must deserialize or copy delivery payload before 
                // delivery handler method returns. Retaining a reference to the payload is not safe: 
                // the memory allocated for it can be deallocated at any moment after the handler returns.
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                    var msg = JsonConvert.DeserializeObject<TMessage>(json);

                    _rabbitMqHub.OnMessageReceived(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Received });
                    try
                    {
                        var redelivered = ea.Redelivered;
                        if (bool.TryParse(ea.BasicProperties.Headers["redelivered"].ToString(), out bool value))
                            redelivered = value ? value : ea.Redelivered;

                        if (handler.Handle(msg, redelivered))
                        {
                            channel.BasicAck(ea.DeliveryTag, false);
                            _rabbitMqHub.OnMessageConsumeOK(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Consumed });
                        }
                        else
                        {
                            channel.BasicNack(ea.DeliveryTag, false, false);
                            _rabbitMqHub.OnMessageConsumeFailed(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Failed });
                        }
                    }
                    catch (Exception ex)
                    {
                        _rabbitMqHub.OnMessageConsumeFailed(new MessageConsumeState
                        {
                            MessagePayloadJson = json,
                            Status = ConsumeStatus.Consumed,
                            Remark = ex.Message
                        });
                    }
                };

                return consumer;
            }

            private AsyncEventingBasicConsumer SetupAsyncConsumer<TMessage, THandler>(IModel channel, THandler handler)
                where THandler : BaseMessageHandler<TMessage>
            {
                // 说明：
                // .NET clients guarantee that deliveries on a single channel will be dispatched in the
                // same order there were received regardless of the degree of concurrency.
                // *but*
                // Note that once dispatched, concurrent processing of deliveries will result in a natural
                // race condition between the threads doing the processing.
                // 
                // link: https://www.rabbitmq.com/docs/consumers#concurrency
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.Received += async (model, ea) =>
                {
                    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                    var msg = JsonConvert.DeserializeObject<TMessage>(json);

                    _rabbitMqHub.OnMessageReceived(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Received });
                    try
                    {
                        var redelivered = ea.Redelivered;
                        if (bool.TryParse(ea.BasicProperties.Headers["redelivered"].ToString(), out bool value))
                            redelivered = value ? value : ea.Redelivered;

                        if (await handler.HandleAsync(msg, redelivered))
                        {
                            channel.BasicAck(ea.DeliveryTag, false);
                            _rabbitMqHub.OnMessageReceived(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Consumed });
                        }
                        else
                        {
                            channel.BasicNack(ea.DeliveryTag, false, false);
                            _rabbitMqHub.OnMessageReceived(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Failed });
                        }
                    }
                    catch (Exception ex)
                    {
                        _rabbitMqHub.OnMessageConsumeFailed(new MessageConsumeState
                        {
                            MessagePayloadJson = json,
                            Status = ConsumeStatus.Consumed,
                            Remark = ex.Message
                        });
                    }
                };

                return consumer;
            }
            #endregion

            public ChannelWrapper GetChannelWrapper(bool publishConfirm = false)
            {
                return _rabbitMqHub.CreateChannel(publishConfirm);
            }

            public void ExchangeDeclare(string exchange, string type, bool durable = true, IDictionary<string, object> arguments = null)
            {
                using (var created = _rabbitMqHub.connection.CreateModel())
                    created.ExchangeDeclare(exchange, type, durable, autoDelete: false, arguments);
            }

            public void QueueDeclare(string queue, bool durable = true, IDictionary<string, object> arguments = null)
            {
                using (var created = _rabbitMqHub.connection.CreateModel())
                    created.QueueDeclare(queue, durable, exclusive: false, autoDelete: false, arguments);
            }

            public void QueueBind(string queue, string exchange, string routeKey, IDictionary<string, object> arguments = null)
            {
                using (var created = _rabbitMqHub.connection.CreateModel())
                    created.QueueBind(queue, exchange, routeKey, arguments);
            }
        }
    }
}
