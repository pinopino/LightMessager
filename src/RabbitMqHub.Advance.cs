using LightMessager.Exceptions;
using LightMessager.Model;
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
        public class Advance
        {
            private RabbitMqHub _rabbitMqHub;
            public Advance(RabbitMqHub rabbitMqHub)
            {
                _rabbitMqHub = rabbitMqHub;
            }

            public void Send<TBody>(TBody messageBody, string exchange, string routeKey, bool mandatory, bool publishConfirm = false, IDictionary<string, object> headers = null)
            {
                if (publishConfirm)
                {
                    using (var pooled = _rabbitMqHub.GetConfirmedChannel() as PooledConfirmedChannel)
                    {
                        var message = new Message<TBody>(messageBody);
                        pooled.Publish(message, exchange, routeKey, mandatory, headers);
                        pooled.WaitForConfirms();
                    }
                }
                else
                {
                    using (var pooled = _rabbitMqHub.GetChannel() as PooledChannel)
                    {
                        var message = new Message<TBody>(messageBody);
                        pooled.Publish(message, exchange, routeKey, mandatory, headers);
                    }
                }
            }

            public void Send<TBody>(IEnumerable<TBody> messageBodys, string exchange, string routeKey, bool mandatory, bool publishConfirm = false, IDictionary<string, object> headers = null)
            {
                if (publishConfirm)
                {
                    using (var pooled = _rabbitMqHub.GetConfirmedChannel() as PooledConfirmedChannel)
                    {
                        var counter = 0;
                        foreach (var messageBody in messageBodys)
                        {
                            var message = new Message<TBody>(messageBody);
                            pooled.Publish(message, exchange, routeKey, mandatory, headers);

                            if (++counter % _rabbitMqHub._batchSize == 0)
                                pooled.WaitForConfirms();
                        }
                        pooled.WaitForConfirms();
                    }
                }
                else
                {
                    using (var pooled = _rabbitMqHub.GetChannel() as PooledChannel)
                    {
                        foreach (var messageBody in messageBodys)
                        {
                            var message = new Message<TBody>(messageBody);
                            pooled.Publish(message, exchange, routeKey, mandatory, headers);
                        }
                    }
                }
            }

            public async Task SendAsync<TBody>(TBody messageBody, string exchange, string routeKey, bool mandatory, bool publishConfirm = false, IDictionary<string, object> headers = null)
            {
                if (publishConfirm)
                {
                    using (var pooled = _rabbitMqHub.GetConfirmedChannel() as PooledConfirmedChannel)
                    {
                        var message = new Message<TBody>(messageBody);
                        var sequence = await pooled.PublishReturnSeqAsync(message, exchange, routeKey, mandatory, headers);
                        await pooled.WaitForConfirmsAsync(sequence);
                    }
                }
                else
                {
                    using (var pooled = _rabbitMqHub.GetChannel() as PooledChannel)
                    {
                        var message = new Message<TBody>(messageBody);
                        await pooled.PublishAsync(message, exchange, routeKey, mandatory, headers);
                    }
                }
            }

            public async Task SendAsync<TBody>(IEnumerable<TBody> messageBodys, string exchange, string routeKey, bool mandatory, bool publishConfirm = false, IDictionary<string, object> headers = null)
            {
                if (publishConfirm)
                {
                    using (var pooled = _rabbitMqHub.GetConfirmedChannel() as PooledConfirmedChannel)
                    {
                        var counter = 0;
                        var sequence = 0ul;
                        foreach (var messageBody in messageBodys)
                        {
                            var message = new Message<TBody>(messageBody);
                            sequence = await pooled.PublishReturnSeqAsync(message, exchange, routeKey, mandatory, headers);

                            if (++counter % _rabbitMqHub._batchSize == 0)
                                await pooled.WaitForConfirmsAsync(sequence);
                        }
                        await pooled.WaitForConfirmsAsync(sequence);
                    }
                }
                else
                {
                    using (var pooled = _rabbitMqHub.GetChannel() as PooledChannel)
                    {
                        foreach (var messageBody in messageBodys)
                        {
                            var message = new Message<TBody>(messageBody);
                            await pooled.PublishAsync(message, exchange, routeKey, mandatory, headers);
                        }
                    }
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
                        var json = Encoding.UTF8.GetString(ea.Body.Span);
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
                        var json = Encoding.UTF8.GetString(ea.Body.Span);
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
    }
}
