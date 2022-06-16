using LightMessager.Exceptions;
using NLog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LightMessager.Model
{
    public abstract class BaseMessageHandler<TBody> : IHandleMessages<TBody>
    {
        private static Logger _logger = LogManager.GetLogger("MessageHandler");

        private int _maxRequeue;
        protected Dictionary<string, int> _requeueCounter;
        internal RabbitMqHub rabbitMqHub;

        protected BaseMessageHandler()
        {
            _maxRequeue = 1;
            _requeueCounter = new Dictionary<string, int>();
        }

        // 说明：redelivered来自于上层传入的BasicDeliverEventArgs.Redelivered
        // 该值为false说明一定没有处理过该条消息，而为true则意义不大没办法说明任何事情
        public bool Handle(Message<TBody> message)
        {
            // 说明：
            // 返回值true或false表达的是对于消息处理的业务结果，跟是否需要重新入队没有任何关系；
            // 库在这里的假设是如果DoHandle内部有异常情况发生则我们可以试着有限的requeue一波（仅针对特定的异常类型），
            // 因此你会看到DoRequeue的逻辑是放在catch(LightMessagerException)处理块中的。
            // 这同时也意味着每一波retry之后是否重新入队我们都需要重新判断，msg.NeedRequeue正是用于此目的。
            // 注意bool值默认为false，如果没有任何异常则NeedRequeue自动为false。
            try
            {
                return DoHandle(message);
            }
            catch (LightMessagerException ex)
            {
                rabbitMqHub.OnMessageConsumeFailed(message.MsgId, $"出错，准备Requeue，异常：{ex.Message}");
                DoRequeue(message);
            }
            catch (Exception ex)
            {
                rabbitMqHub.OnMessageConsumeFailed(message.MsgId, ex.Message);
            }

            return true;
        }

        public Task<bool> HandleAsync(Message<TBody> message)
        {
            try
            {
                return DoHandleAsync(message);
            }
            catch (LightMessagerException ex)
            {
                rabbitMqHub.OnMessageConsumeFailed(message.MsgId, $"出错，准备Requeue，异常：{ex.Message}");
                DoRequeue(message);
            }
            catch (Exception ex)
            {
                rabbitMqHub.OnMessageConsumeFailed(message.MsgId, ex.Message);
            }

            return Task.FromResult(true);
        }

        protected virtual bool DoHandle(Message<TBody> message)
        {
            throw new NotImplementedException();
        }

        protected virtual Task<bool> DoHandleAsync(Message<TBody> message)
        {
            throw new NotImplementedException();
        }

        private void DoRequeue(Message<TBody> message)
        {
            var msgId = message.MsgId;
            if (_requeueCounter.TryGetValue(msgId, out int requeueCount))
            {
                message.NeedRequeue = requeueCount < _maxRequeue;
                if (message.NeedRequeue)
                {
                    _requeueCounter[msgId] = requeueCount + 1;
                    _logger.Debug($"消息准备第[{requeueCount + 1}]次requeue");
                }
                else
                {
                    var remark = "消息requeue次数已达上限，不再尝试";
                    _logger.Warn(remark);
                }
            }
            else
            {
                message.NeedRequeue = true;
                _requeueCounter[msgId] = 1;
                _logger.Debug($"消息准备第[1]次requeue");
            }
        }
    }
}
