using LightMessager.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LightMessager
{
    public sealed partial class RabbitMqHub
    {
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
                    EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out QueueInfo info);
                    pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                }
                return pooled.WaitForConfirms();
            }
        }

        public bool Send<TMessage>(IEnumerable<TMessage> messages, string routeKey = "", int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                QueueInfo info = null;
                if (string.IsNullOrEmpty(routeKey))
                    EnsureSendQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                else
                    EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);

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

        public bool Send<TMessage>(IEnumerable<TMessage> messages, Func<TMessage, string> routeKeySelector, int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (routeKeySelector == null)
                throw new ArgumentNullException("routeKeySelector");

            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                QueueInfo info = null;
                EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);

                var counter = 0;
                foreach (var message in messages)
                {
                    if (string.IsNullOrWhiteSpace(message.MsgId))
                        throw new ArgumentNullException("message.MsgId");

                    if (!PreTrackMessage(message))
                        return false;

                    var routekey = routeKeySelector(message);
                    pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routekey);

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
                    EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out QueueInfo info);
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
                    EnsureSendQueue(pooled.Channel, typeof(TMessage), delaySend, out info);
                else
                    EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);

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

        public async Task<bool> SendAsync<TMessage>(IEnumerable<TMessage> messages, Func<TMessage, string> routeKeySelector, int delaySend = 0)
            where TMessage : BaseMessage
        {
            if (routeKeySelector == null)
                throw new ArgumentNullException("routeKeySelector");

            if (!messages.Any())
                return false;

            using (var pooled = _channel_pools.Get() as PooledChannel)
            {
                QueueInfo info = null;
                EnsureRouteQueue(pooled.Channel, typeof(TMessage), delaySend, out info);

                ulong sequence = 0;
                TMessage lastMsg = null;
                foreach (var message in messages)
                {
                    if (string.IsNullOrWhiteSpace(message.MsgId))
                        throw new ArgumentNullException("message.MsgId");

                    if (!PreTrackMessage(message))
                        return false;

                    lastMsg = message;
                    var routekey = routeKeySelector(message);
                    sequence = pooled.PublishAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routekey);
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
                if (delaySend == 0)
                    pooled.Publish(message, info.Exchange, string.Empty);
                else
                    pooled.Publish(message, string.Empty, info.Delay_Queue);
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

                    if (delaySend == 0)
                        pooled.Publish(message, info.Exchange, string.Empty);
                    else
                        pooled.Publish(message, string.Empty, info.Delay_Queue);

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
                if (delaySend == 0)
                    sequence = pooled.PublishAsync(message, info.Exchange, string.Empty);
                else
                    sequence = pooled.PublishAsync(message, string.Empty, info.Delay_Queue);
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
                    if (delaySend == 0)
                        sequence = pooled.PublishAsync(message, info.Exchange, string.Empty);
                    else
                        sequence = pooled.PublishAsync(message, string.Empty, info.Delay_Queue);
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
