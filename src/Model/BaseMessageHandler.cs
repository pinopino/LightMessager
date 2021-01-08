using LightMessager.Common;
using LightMessager.Exceptions;
using LightMessager.Track;
using NLog;
using System;
using System.Threading.Tasks;

namespace LightMessager.Model
{
    public abstract class BaseMessageHandler<TMessage> : IHandleMessages<TMessage>
        where TMessage : BaseMessage
    {
        private int _maxRetry;
        private int _maxRequeue;
        private int _backoffMs;
        private static Logger _logger = LogManager.GetLogger("MessageHandler");

        internal BaseMessageTracker Tracker { set; get; }
        public abstract bool Idempotent { get; }

        protected BaseMessageHandler()
        {
            _maxRetry = 1; // 按需修改
            _maxRequeue = 2;
            _backoffMs = 200;
        }

        public bool Handle(TMessage message)
        {
            try
            {
                // 执行DoHandle可能会发生异常，如果是我们特定的异常则进行重试操作
                // 否则直接抛出异常
                var ret = RetryHelper.Retry(() => DoHandle(message), Idempotent ? _maxRetry : 1, _backoffMs, p =>
                {
                    var ex = p as Exception<LightMessagerExceptionArgs>;
                    if (ex != null)
                        return true;

                    return false;
                });

                if (ret)
                    MarkConsumed(message);
                return ret;
            }
            catch (Exception ex)
            {
                _logger.Debug("未知异常：" + ex.Message + "；堆栈：" + ex.StackTrace);
                // 说明：_maxRetry次之后还需要判该条消息requeue次数是否超过允
                // 许的最大值：如果是，不再做任何进一步尝试了，log一波；否则
                // 设置NeedRequeue为true准备重新入队列
                DoRequeue(message);
            }
            return false;
        }

        public async Task<bool> HandleAsync(TMessage message)
        {
            try
            {
                var ret = await RetryHelper.RetryAsync(async () => await DoHandleAsync(message),
                    Idempotent ? _maxRetry : 1, _backoffMs, p =>
                {
                    var ex = p as Exception<LightMessagerExceptionArgs>;
                    if (ex != null)
                        return true;

                    return false;
                });

                if (ret)
                    MarkConsumed(message);

                return ret;
            }
            catch (Exception ex)
            {
                _logger.Debug("未知异常：" + ex.Message + "；堆栈：" + ex.StackTrace);
                DoRequeue(message);
            }
            return false;
        }

        protected virtual bool DoHandle(TMessage message)
        {
            throw new NotImplementedException();
        }

        protected virtual Task<bool> DoHandleAsync(TMessage message)
        {
            throw new NotImplementedException();
        }

        private void DoRequeue(TMessage message)
        {
            var model = Tracker.GetMessage(message.MsgId);
            message.NeedRequeue = Idempotent && model.Requeue < _maxRequeue;
            if (message.NeedRequeue)
            {
                message.Requeue += 1;
                _logger.Debug($"消息准备requeue，当前requeue次数[{message.Requeue}]");
            }
            else
            {
                _logger.Debug("消息处理端不支持幂等或requeue已达上限，不再尝试");
                Tracker.SetStatus(message.MsgId, newStatus: MessageState.Error, oldStatus: MessageState.Confirmed);
            }
        }

        private void MarkConsumed(TMessage message)
        {
            Tracker.SetStatus(message.MsgId, newStatus: MessageState.Consumed, oldStatus: MessageState.Confirmed);
        }
    }
}
