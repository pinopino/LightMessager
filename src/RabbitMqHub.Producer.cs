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
        public bool Send<TBody>(TBody messageBody, string routeKey = "", int delaySend = 0)
        {
            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                var message = new Message<TBody>(messageBody);
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSendQueue(pooled.Channel, typeof(TBody), delaySend, out QueueInfo info);
                    pooled.Publish(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                }
                else
                {
                    QueueInfo info;
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopicQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                    else
                        EnsureRouteQueue(pooled.Channel, typeof(TBody), delaySend, out info);
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
        public bool Send<TBody>(IEnumerable<TBody> messageBodys, string routeKey = "", int delaySend = 0)
        {
            if (!messageBodys.Any())
                return false;

            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                QueueInfo info;
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSendQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                }
                else
                {
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopicQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                    else
                        EnsureRouteQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                }

                var counter = 0;
                foreach (var messageBody in messageBodys)
                {
                    var message = new Message<TBody>(messageBody);
                    if (string.IsNullOrEmpty(routeKey))
                        pooled.Publish(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                    else
                        pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);

                    if (++counter % _batchSize == 0)
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
        private bool Send<TBody>(IEnumerable<TBody> messageBodys, Func<TBody, string> routeKeySelector, int delaySend = 0)
        {
            // 可见性调整为private。
            // 感觉这个方法挺危险的而实际干的事情其实是将上层业务可以做的事情也一并做了
            // 造成库内部一定的复杂性，我还需要再多权衡下。下同
            if (routeKeySelector == null)
                throw new ArgumentNullException("routeKeySelector");

            if (!messageBodys.Any())
                return false;

            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                QueueInfo info;
                var counter = 0;
                foreach (var messageBody in messageBodys)
                {
                    var routeKey = routeKeySelector(messageBody);
                    var message = new Message<TBody>(messageBody);
                    if (string.IsNullOrEmpty(routeKey))
                    {
                        EnsureSendQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                        pooled.Publish(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                    }
                    else
                    {
                        if (routeKey.IndexOf('.') > 0)
                            EnsureTopicQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                        else
                            EnsureRouteQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                        pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                    }

                    if (++counter % _batchSize == 0)
                        pooled.WaitForConfirms();
                }
                return pooled.WaitForConfirms();
            }
        }

        public async Task<bool> SendAsync<TBody>(TBody messageBody, string routeKey = "", int delaySend = 0)
        {
            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                var sequence = 0ul;
                var message = new Message<TBody>();
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSendQueue(pooled.Channel, typeof(TBody), delaySend, out QueueInfo info);
                    sequence = await pooled.PublishReturnSeqAsync(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                }
                else
                {
                    QueueInfo info;
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopicQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                    else
                        EnsureRouteQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                    sequence = await pooled.PublishReturnSeqAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                }
                return await pooled.WaitForConfirmsAsync(sequence);
            }
        }

        public async Task<bool> SendAsync<TBody>(IEnumerable<TBody> messageBodys, string routeKey = "", int delaySend = 0)
        {
            if (!messageBodys.Any())
                return false;

            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                QueueInfo info = null;
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSendQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                }
                else
                {
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopicQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                    else
                        EnsureRouteQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                }

                var counter = 0;
                var sequence = 0ul;
                foreach (var messageBody in messageBodys)
                {
                    var message = new Message<TBody>(messageBody);
                    if (string.IsNullOrEmpty(routeKey))
                        sequence = await pooled.PublishReturnSeqAsync(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                    else
                        sequence = await pooled.PublishReturnSeqAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);

                    if (++counter % _batchSize == 0)
                        await pooled.WaitForConfirmsAsync(sequence);
                }
                return await pooled.WaitForConfirmsAsync(sequence);
            }
        }

        private async Task<bool> SendAsync<TBody>(IEnumerable<TBody> messageBodys, Func<TBody, string> routeKeySelector, int delaySend = 0)
        {
            if (routeKeySelector == null)
                throw new ArgumentNullException("routeKeySelector");

            if (!messageBodys.Any())
                return false;

            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                QueueInfo info = null;
                var counter = 0;
                var sequence = 0ul;
                foreach (var messageBody in messageBodys)
                {
                    var routeKey = routeKeySelector(messageBody);
                    var message = new Message<TBody>(messageBody);
                    if (string.IsNullOrEmpty(routeKey))
                    {
                        EnsureSendQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                        sequence = await pooled.PublishReturnSeqAsync(message, string.Empty, delaySend == 0 ? info.Queue : info.Delay_Queue);
                    }
                    else
                    {
                        if (routeKey.IndexOf('.') > 0)
                            EnsureTopicQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                        else
                            EnsureRouteQueue(pooled.Channel, typeof(TBody), delaySend, out info);
                        sequence = await pooled.PublishReturnSeqAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, routeKey);
                    }

                    if (++counter % _batchSize == 0)
                        await pooled.WaitForConfirmsAsync(sequence);
                }
                return await pooled.WaitForConfirmsAsync(sequence);
            }
        }

        public bool Publish<TBody>(TBody messageBody, int delaySend = 0)
        {
            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                EnsurePublishQueue(pooled.Channel, typeof(TBody), delaySend, out QueueInfo info);
                var message = new Message<TBody>(messageBody);
                pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, string.Empty);
                return pooled.WaitForConfirms();
            }
        }

        public bool Publish<TBody>(IEnumerable<TBody> messageBodys, int delaySend = 0)
        {
            if (!messageBodys.Any())
                return false;

            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                EnsurePublishQueue(pooled.Channel, typeof(TBody), delaySend, out QueueInfo info);

                var counter = 0;
                foreach (var messageBody in messageBodys)
                {
                    var message = new Message<TBody>(messageBody);
                    pooled.Publish(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, string.Empty);

                    if (++counter % _batchSize == 0)
                        pooled.WaitForConfirms();
                }
                return pooled.WaitForConfirms();
            }
        }

        public async Task<bool> PublishAsync<TBody>(TBody messageBody, int delaySend = 0)
        {
            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                var sequence = 0ul;
                EnsurePublishQueue(pooled.Channel, typeof(TBody), delaySend, out QueueInfo info);
                var message = new Message<TBody>(messageBody);
                sequence = await pooled.PublishReturnSeqAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, string.Empty);
                return await pooled.WaitForConfirmsAsync(sequence);
            }
        }

        public async Task<bool> PublishAsync<TBody>(IEnumerable<TBody> messageBodys, int delaySend = 0)
        {
            if (!messageBodys.Any())
                return false;

            using (var pooled = GetConfirmedChannel() as PooledConfirmedChannel)
            {
                EnsurePublishQueue(pooled.Channel, typeof(TBody), delaySend, out QueueInfo info);

                var counter = 0;
                var sequence = 0ul;
                foreach (var messageBody in messageBodys)
                {
                    var message = new Message<TBody>(messageBody);
                    sequence = await pooled.PublishReturnSeqAsync(message, delaySend == 0 ? info.Exchange : info.Delay_Exchange, string.Empty);

                    if (++counter % _batchSize == 0)
                        pooled.WaitForConfirms();
                }
                return await pooled.WaitForConfirmsAsync(sequence);
            }
        }
    }
}
