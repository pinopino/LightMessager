using LightMessager.Model;
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
        /// <param name="message">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delay">延迟发送（单位秒）</param>
        /// <returns>true发送成功，反之失败</returns>
        public void Send(object message, string routeKey = "", int delay = 0)
        {
            using (var pooled = GetConfirmChannelFromPool() as PooledConfirmChannel)
            {
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSend(pooled.Channel, message.GetType(), delay, out QueueInfo info, out DelayQueueInfo delayInfo);
                    pooled.Publish(message, string.Empty, delay == 0 ? info.Queue : delayInfo.DelayQueue, false, null);
                }
                else
                {
                    QueueInfo info;
                    DelayQueueInfo delayInfo;
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopic(pooled.Channel, message.GetType(), routeKey, delay, out info, out delayInfo);
                    else
                        EnsureRoute(pooled.Channel, message.GetType(), routeKey, delay, out info, out delayInfo);

                    if (delay > 0)
                        pooled.Publish(message, string.Empty, delayInfo.DelayQueue, false, null);
                    else
                        pooled.Publish(message, info.Exchange, info.Queue, false, null);
                }
                pooled.WaitForConfirms();
            }
        }

        /// <summary>
        /// 发送一批消息
        /// </summary>
        /// <param name="messages">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delay">延迟发送（单位秒）</param>
        /// <returns>true发送成功，反之失败</returns>
        public void Send(IEnumerable<object> messages, string routeKey = "", int delay = 0)
        {
            if (!messages.Any())
                return;

            using (var pooled = GetConfirmChannelFromPool() as PooledConfirmChannel)
            {
                QueueInfo info;
                DelayQueueInfo delayInfo;
                if (string.IsNullOrEmpty(routeKey))
                {
                    EnsureSend(pooled.Channel, messages.First().GetType(), delay, out info, out delayInfo);
                }
                else
                {
                    if (routeKey.IndexOf('.') > 0)
                        EnsureTopic(pooled.Channel, messages.First().GetType(), routeKey, delay, out info, out delayInfo);
                    else
                        EnsureRoute(pooled.Channel, messages.First().GetType(), routeKey, delay, out info, out delayInfo);
                }

                var counter = 0;
                foreach (var message in messages)
                {
                    if (string.IsNullOrEmpty(routeKey))
                    {
                        pooled.Publish(message, string.Empty, delay == 0 ? info.Queue : delayInfo.DelayQueue, false, null);
                    }
                    else
                    {
                        if (delay > 0)
                            pooled.Publish(message, string.Empty, delayInfo.DelayQueue, false, null);
                        else
                            pooled.Publish(message, info.Exchange, routeKey, false, null);
                    }

                    if (++counter % _batchSize == 0)
                        pooled.WaitForConfirms();
                }
                pooled.WaitForConfirms();
            }
        }

        /// <summary>
        /// 异步发送一条消息
        /// </summary>
        /// <param name="message">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delay">延迟发送（单位秒）</param>
        /// <returns>true发送成功，反之失败</returns>
        public Task SendAsync(object message, string routeKey = "", int delay = 0)
        {
            return Task.Factory.StartNew(() =>
            {
                Send(message, routeKey, delay);
            });
        }

        /// <summary>
        /// 异步发送一批消息
        /// </summary>
        /// <param name="messages">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delay">延迟发送（单位秒）</param>
        /// <returns>true发送成功，反之失败</returns>
        public Task SendAsync(IEnumerable<object> messages, string routeKey = "", int delay = 0)
        {
            return Task.Factory.StartNew(() =>
            {
                Send(messages, routeKey, delay);
            });
        }

        /// <summary>
        /// 广播一条消息
        /// </summary>
        /// <param name="message">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delay">延迟发送（单位秒）</param>
        /// <returns>true发送成功，反之失败</returns>
        public void Publish(object message, int delay = 0)
        {
            using (var pooled = GetConfirmChannelFromPool() as PooledConfirmChannel)
            {
                EnsurePublish(pooled.Channel, message.GetType(), delay, out QueueInfo info, out DelayQueueInfo delayInfo);
                if (delay > 0)
                    pooled.Publish(message, string.Empty, delayInfo.DelayQueue, false, null);
                else
                    pooled.Publish(message, info.Exchange, string.Empty, false, null);

                pooled.WaitForConfirms();
            }
        }

        /// <summary>
        /// 广播一批消息
        /// </summary>
        /// <param name="messages">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delay">延迟发送（单位秒）</param>
        /// <returns>true发送成功，反之失败</returns>
        public void Publish(IEnumerable<object> messages, int delay = 0)
        {
            if (!messages.Any())
                return;

            using (var pooled = GetConfirmChannelFromPool() as PooledConfirmChannel)
            {
                EnsurePublish(pooled.Channel, messages.First().GetType(), delay, out QueueInfo info, out DelayQueueInfo delayInfo);

                var counter = 0;
                foreach (var message in messages)
                {
                    if (delay > 0)
                        pooled.Publish(message, string.Empty, delayInfo.DelayQueue, false, null);
                    else
                        pooled.Publish(message, info.Exchange, string.Empty, false, null);

                    if (++counter % _batchSize == 0)
                        pooled.WaitForConfirms();
                }
                pooled.WaitForConfirms();
            }
        }

        /// <summary>
        /// 异步广播一条消息
        /// </summary>
        /// <param name="message">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delay">延迟发送（单位秒）</param>
        /// <returns>true发送成功，反之失败</returns>
        public Task PublishAsync(object message, int delay = 0)
        {
            return Task.Factory.StartNew(() =>
            {
                Publish(message, delay);
            });
        }

        /// <summary>
        /// 异步广播一批消息
        /// </summary>
        /// <param name="messages">要发送的消息</param>
        /// <param name="routeKey">可以是普通的字符串也可以是以.分割的字符串，后者将自动走topic方式发送</param>
        /// <param name="delay">延迟发送（单位秒）</param>
        /// <returns>true发送成功，反之失败</returns>
        public Task PublishAsync(IEnumerable<object> messages, int delay = 0)
        {
            return Task.Factory.StartNew(() =>
            {
                Publish(messages, delay);
            });
        }
    }
}
