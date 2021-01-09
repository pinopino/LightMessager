using LightMessager.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LightMessager
{
    public sealed partial class RabbitMqHub
    {
        /// <summary>
        /// 发送一条消息
        /// </summary>
        /// <typeparam name="TMessage">继承自BaseMessage</typeparam>
        /// <param name="message">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delaySend">延迟发送</param>
        /// <returns>true发送成功，反之失败</returns>
        public bool Send<TMessage>(TMessage message, string routeKey = "", int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (string.IsNullOrWhiteSpace(message.MsgId))
                throw new ArgumentNullException("message.MsgId");

            if (!PreTrackMessage(message))
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSendQueue(pooled.Channel, typeof(TMessage), delaySend, out QueueInfo info);
                    pooled.Publish(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                }
                else
                {
                    QueueInfo info;
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopicQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                    else
                        EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                    pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                }
                return pooled.WaitForConfirms();
            }
        }

        /// <summary>
        /// 发送一批消息
        /// </summary>
        /// <typeparam name="TMessage">继承自BaseMessage</typeparam>
        /// <param name="messages">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delaySend">延迟发送</param>
        /// <returns>true发送成功，反之失败</returns>
        public bool Send<TMessage>(IEnumerable<TMessage> messages, string routeKey = "", int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                QueueInfo info;
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSendQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                }
                else
                {
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopicQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                    else
                        EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                }

                var counter = 0;
                foreach (var message in messages)
                {
                    counter++;
                    if (string.IsNullOrWhiteSpace(message.MsgId))
                        throw new ArgumentNullException("message.MsgId");

                    if (!PreTrackMessage(message))
                        return false;

                    if (string.IsNullOrEmpty(routeKey))
                        pooled.Publish(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                    else
                        pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);

                    if (counter % _batch_size == 0)
                        pooled.WaitForConfirms();
                }
                return pooled.WaitForConfirms();
            }
        }

        /// <summary>
        /// 发送一批消息
        /// </summary>
        /// <typeparam name="TMessage">继承自BaseMessage</typeparam>
        /// <param name="messages">要发送的消息</param>
        /// <param name="routeKeySelector">从消息本身获取routekey的委托，返回值可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delaySend">延迟发送</param>
        /// <returns>true发送成功，反之失败</returns>
        private bool Send<TMessage>(IEnumerable<TMessage> messages, Func<TMessage, string> routeKeySelector, int delaySend = 0)
            where TMessage : BaseMessage
        {
            // 可见性调整为private。
            // 感觉这个方法挺危险的而实际干的事情其实是将上层业务可以做的事情也一并做了
            // 造成库内部一定的复杂性，我还需要再多权衡下。下同
            if (routeKeySelector == null)
                throw new ArgumentNullException("routeKeySelector");

            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                QueueInfo info;
                var counter = 0;
                foreach (var message in messages)
                {
                    if (string.IsNullOrWhiteSpace(message.MsgId))
                        throw new ArgumentNullException("message.MsgId");

                    if (!PreTrackMessage(message))
                        return false;

                    var routeKey = routeKeySelector(message);
                    if (string.IsNullOrEmpty(routeKey))
                    {
                        EnsureSendQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                        pooled.Publish(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                    }
                    else
                    {
                        if (routeKey.IndexOf('.') > 0)
                            EnsureTopicQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                        else
                            EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                        pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                    }

                    if (counter % _batch_size == 0)
                        pooled.WaitForConfirms();
                }
                return pooled.WaitForConfirms();
            }
        }

        public async Task<bool> SendAsync<TMessage>(TMessage message, string routeKey = "", int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (string.IsNullOrWhiteSpace(message.MsgId))
                throw new ArgumentNullException("message.MsgId");

            if (!PreTrackMessage(message))
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                ulong sequence = 0;
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSendQueue(pooled.Channel, typeof(TMessage), delaySend, out QueueInfo info);
                    sequence = pooled.PublishAsync(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                }
                else
                {
                    QueueInfo info;
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopicQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                    else
                        EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                    sequence = pooled.PublishAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                }
                await pooled.WaitForConfirmsAsync(sequence, message.MsgId);
                return true;
            }
        }

        public async Task<bool> SendAsync<TMessage>(IEnumerable<TMessage> messages, string routeKey = "", int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                QueueInfo info = null;
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSendQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                }
                else
                {
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopicQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                    else
                        EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                }

                ulong sequence = 0;
                TMessage lastMsg = null;
                foreach (var message in messages)
                {
                    if (string.IsNullOrWhiteSpace(message.MsgId))
                        throw new ArgumentNullException("message.MsgId");

                    if (!PreTrackMessage(message))
                        return false;

                    lastMsg = message;
                    if (string.IsNullOrEmpty(routeKey))
                        sequence = pooled.PublishAsync(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                    else
                        sequence = pooled.PublishAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                }
                await pooled.WaitForConfirmsAsync(sequence, lastMsg.MsgId);
                return true;
            }
        }

        private async Task<bool> SendAsync<TMessage>(IEnumerable<TMessage> messages, Func<TMessage, string> routeKeySelector, int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (routeKeySelector == null)
                throw new ArgumentNullException("routeKeySelector");

            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                QueueInfo info = null;
                ulong sequence = 0;
                TMessage lastMsg = null;
                foreach (var message in messages)
                {
                    if (string.IsNullOrWhiteSpace(message.MsgId))
                        throw new ArgumentNullException("message.MsgId");

                    if (!PreTrackMessage(message))
                        return false;

                    lastMsg = message;
                    var routeKey = routeKeySelector(message);
                    if (string.IsNullOrEmpty(routeKey))
                    {
                        EnsureSendQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                        sequence = pooled.PublishAsync(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                    }
                    else
                    {
                        if (routeKey.IndexOf('.') > 0)
                            EnsureTopicQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                        else
                            EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                        sequence = pooled.PublishAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                    }

                }
                await pooled.WaitForConfirmsAsync(sequence, lastMsg.MsgId);
                return true;
            }
        }

        public bool Publish<TMessage>(TMessage message, int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (string.IsNullOrWhiteSpace(message.MsgId))
                throw new ArgumentNullException("message.MsgId");

            if (!PreTrackMessage(message))
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                EnsurePublishQueue(pooled.Channel, typeof(TMessage), delaySend, out QueueInfo info);
                pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, string.Empty);
                return pooled.WaitForConfirms();
            }
        }

        public bool Publish<TMessage>(IEnumerable<TMessage> messages, int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                EnsurePublishQueue(pooled.Channel, typeof(TMessage), delaySend, out QueueInfo info);

                var counter = 0;
                foreach (var message in messages)
                {
                    if (string.IsNullOrWhiteSpace(message.MsgId))
                        throw new ArgumentNullException("message.MsgId");

                    if (!PreTrackMessage(message))
                        return false;

                    pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, string.Empty);

                    if (counter % _batch_size == 0)
                        pooled.WaitForConfirms();
                }
                return pooled.WaitForConfirms();
            }
        }

        public async Task<bool> PublishAsync<TMessage>(TMessage message, int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (string.IsNullOrWhiteSpace(message.MsgId))
                throw new ArgumentNullException("message.MsgId");

            if (!PreTrackMessage(message))
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                ulong sequence = 0;
                EnsurePublishQueue(pooled.Channel, typeof(TMessage), delaySend, out QueueInfo info);
                sequence = pooled.PublishAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, string.Empty);
                await pooled.WaitForConfirmsAsync(sequence, message.MsgId);
                return true;
            }
        }

        public async Task<bool> PublishAsync<TMessage>(IEnumerable<TMessage> messages, int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                EnsurePublishQueue(pooled.Channel, typeof(TMessage), delaySend, out QueueInfo info);

                ulong sequence = 0;
                TMessage lastMsg = null;
                foreach (var message in messages)
                {
                    if (string.IsNullOrWhiteSpace(message.MsgId))
                        throw new ArgumentNullException("message.MsgId");

                    if (!PreTrackMessage(message))
                        return false;

                    lastMsg = message;
                    sequence = pooled.PublishAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, string.Empty);
                }
                await pooled.WaitForConfirmsAsync(sequence, lastMsg.MsgId);
                return true;
            }
        }

        public HubConnector<TMessage> GetConnector<TMessage>(int delaySend = 0)
            where TMessage : BaseMessage
        {
            return new HubConnector<TMessage>(this, _channel_pools.Get(), delaySend);
        }
    }
}
