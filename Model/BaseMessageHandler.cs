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
        private bool _idempotent;
        private BaseMessageTracker _tracker;
        private static Logger _logger = LogManager.GetLogger("MessageHandler");

        public bool Idempotent { get { return _idempotent; } }
        internal long Counter;

        protected BaseMessageHandler(BaseMessageTracker tracker, bool idempotent = false)
        {
            _maxRetry = 1; // 按需修改
            _maxRequeue = 2;
            _backoffMs = 200;
            _idempotent = idempotent;
            _tracker = tracker;
        }

        public void Handle(TMessage message)
        {
            try
            {
                // 执行DoHandle可能会发生异常，如果是我们特定的异常则进行重试操作
                // 否则直接抛出异常
                RetryHelper.Retry(() => DoHandle(message), _idempotent ? _maxRetry : 1, _backoffMs, p =>
                {
                    var ex = p as Exception<LightMessagerExceptionArgs>;
                    if (ex != null)
                        return true;

                    return false;
                });

                MarkConsumed(message);
            }
            catch (Exception ex)
            {
                _logger.Debug("未知异常：" + ex.Message + "；堆栈：" + ex.StackTrace);
                // 说明：_maxRetry次之后还需要判该条消息requeue次数是否超过允
                // 许的最大值：如果是，不再做任何进一步尝试了，log一波；否则
                // 设置NeedRequeue为true准备重新入队列定
                DoRequeue(message);
            }
        }

        public async Task HandleAsync(TMessage message)
        {
            try
            {
                await RetryHelper.RetryAsync(() => DoHandle(message), _idempotent ? _maxRetry : 1, _backoffMs, p =>
                {
                    var ex = p as Exception<LightMessagerExceptionArgs>;
                    if (ex != null)
                        return true;

                    return false;
                });

                MarkConsumed(message);
            }
            catch (Exception ex)
            {
                _logger.Debug("未知异常：" + ex.Message + "；堆栈：" + ex.StackTrace);
                DoRequeue(message);
            }
        }

        protected abstract void DoHandle(TMessage message);

        private void DoRequeue(TMessage message)
        {
            var model = _tracker.GetMessage(message.MsgId);
            message.NeedRequeue = _idempotent && model.Requeue < _maxRequeue;
            if (message.NeedRequeue)
            {
                message.Requeue += 1;
                _logger.Debug($"消息准备requeue，当前requeue次数[{message.Requeue}]");
            }
            else
            {
                _logger.Debug("消息requeue已达上限，不再尝试");
                _tracker.SetStatus(message.MsgId, newStatus: MessageState.Error, oldStatus: MessageState.Confirmed);
            }
        }

        private void MarkConsumed(TMessage message)
        {
            Counter++;
            _tracker.SetStatus(message.MsgId, newStatus: MessageState.Consumed, oldStatus: MessageState.Confirmed);
        }
    }
}
