using LightMessager.Model;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading.Tasks;

namespace LightMessager
{
    public sealed partial class RabbitMqHub
    {
        public void Consume<TMessage>(Action<TMessage> action)
        {
            var channel = this.connection.CreateModel();
            channel.BasicQos(0, this._prefetchCount, false);

            EnsureSendQueue(channel, typeof(TMessage), out QueueInfo info);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                InnerHandle(channel, ea, action);
            };
            channel.BasicConsume(info.Queue, false, consumer);
        }

        public void Consume<TMessage>(Action<TMessage> action, params string[] routeKeys)
        {
            if (routeKeys == null || routeKeys.Length == 0)
            {
                Consume(action);
                return;
            }

            var channel = this.connection.CreateModel();
            channel.BasicQos(0, this._prefetchCount, false);
            var res = channel.QueueDeclare();

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
                EnsureTopicExchange(channel, typeof(TMessage), out info);
            else
                EnsureRouteExchange(channel, typeof(TMessage), out info);
            for (var i = 0; i < routeKeys.Length; i++)
                channel.QueueBind(res.QueueName, info.Exchange, routeKeys[i]);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                InnerHandle(channel, ea, action);
            };
            channel.BasicConsume(res.QueueName, false, consumer);
        }

        public void Consume<TMessage>(Action<TMessage> action, string queue)
        {
            var channel = this.connection.CreateModel();
            channel.BasicQos(0, this._prefetchCount, false);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                InnerHandle(channel, ea, action);
            };
            channel.BasicConsume(queue, false, consumer);
        }

        public void Consume<TMessage>(Action<TMessage> action, string exchange, params string[] routeKeys)
        {
            var channel = this.connection.CreateModel();
            channel.BasicQos(0, this._prefetchCount, false);

            var res = channel.QueueDeclare();
            if (routeKeys != null)
            {
                for (var i = 0; i < routeKeys.Length; i++)
                    channel.QueueBind(res.QueueName, exchange, routeKeys[i]);
            }

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                InnerHandle(channel, ea, action);
            };
            channel.BasicConsume(res.QueueName, false, consumer);
        }

        private void InnerHandle<TMessage>(IModel channel, BasicDeliverEventArgs ea, Action<TMessage> action)
        {
            var msgId = ea.BasicProperties.MessageId;
            var json = string.Empty;
            var flag = false;
            var remark = string.Empty;
            try
            {
                json = Encoding.UTF8.GetString(ea.Body.ToArray());
                var msg = JsonConvert.DeserializeObject<TMessage>(json);

                this.OnMessageReceived(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Received });
                action(msg);
                this.OnMessageConsumeOK(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Consumed });
                channel.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                flag = true;
                remark = ex.Message;
            }

            if (flag)
            {
                channel.BasicNack(ea.DeliveryTag, false, false);
                this.OnMessageConsumeFailed(new MessageConsumeState
                {
                    MessagePayloadJson = json,
                    Status = ConsumeStatus.Failed,
                    Remark = remark
                });
            }
        }

        public void Consume<TMessage>(Func<TMessage, Task> func)
        {
            var channel = this.asynConnection.CreateModel();
            channel.BasicQos(0, this._prefetchCount, false);

            EnsureSendQueue(channel, typeof(TMessage), out QueueInfo info);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                await InnerHandle(channel, ea, func);
            };
            channel.BasicConsume(info.Queue, false, consumer);
        }

        public void Consume<TMessage>(Func<TMessage, Task> func, params string[] routeKeys)
        {
            if (routeKeys == null || routeKeys.Length == 0)
            {
                Consume(func);
                return;
            }

            var channel = this.connection.CreateModel();
            channel.BasicQos(0, this._prefetchCount, false);
            var res = channel.QueueDeclare();

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
                EnsureTopicExchange(channel, typeof(TMessage), out info);
            else
                EnsureRouteExchange(channel, typeof(TMessage), out info);
            for (var i = 0; i < routeKeys.Length; i++)
                channel.QueueBind(res.QueueName, info.Exchange, routeKeys[i]);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                await InnerHandle(channel, ea, func);
            };
            channel.BasicConsume(res.QueueName, false, consumer);
        }

        public void Consume<TMessage>(Func<TMessage, Task> func, string queue)
        {
            var channel = this.asynConnection.CreateModel();
            channel.BasicQos(0, this._prefetchCount, false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                await InnerHandle(channel, ea, func);
            };
            channel.BasicConsume(queue, false, consumer);
        }

        public void Consume<TMessage>(Func<TMessage, Task> func, string exchange, params string[] routeKeys)
        {
            var channel = this.asynConnection.CreateModel();
            channel.BasicQos(0, this._prefetchCount, false);

            var res = channel.QueueDeclare();
            if (routeKeys != null)
            {
                for (var i = 0; i < routeKeys.Length; i++)
                    channel.QueueBind(res.QueueName, exchange, routeKeys[i]);
            }

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                await InnerHandle(channel, ea, func);
            };
            channel.BasicConsume(res.QueueName, false, consumer);
        }

        private async Task InnerHandle<TMessage>(IModel channel, BasicDeliverEventArgs ea, Func<TMessage, Task> func)
        {
            var msgId = ea.BasicProperties.MessageId;
            var json = string.Empty;
            var flag = false;
            var remark = string.Empty;
            try
            {
                json = Encoding.UTF8.GetString(ea.Body.ToArray());
                var msg = JsonConvert.DeserializeObject<TMessage>(json);

                this.OnMessageReceived(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Received });
                await func(msg);
                this.OnMessageConsumeOK(new MessageConsumeState { MessagePayloadJson = json, Status = ConsumeStatus.Consumed });
                channel.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                flag = true;
                remark = ex.Message;
            }

            if (flag)
            {
                channel.BasicNack(ea.DeliveryTag, false, false);
                this.OnMessageConsumeFailed(new MessageConsumeState
                {
                    MessagePayloadJson = json,
                    Status = ConsumeStatus.Failed,
                    Remark = remark
                });
            }
        }
    }
}
