using LightMessager.Common;
using LightMessager.Exceptions;
using LightMessager.Track;
using NLog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LightMessager.Model
{
    public abstract class BaseMessageHandler<TBody> : IHandleMessages<TBody>
    {
        private static Logger _logger = LogManager.GetLogger("MessageHandler");

        private int _maxRetry;
        private int _maxRequeue;
        private int _backoffMs;
        internal InMemoryRecvTracker _tracker;
        protected Dictionary<string, int> _requeueCounter;

        internal InMemoryRecvTracker Tracker => _tracker;
        public abstract bool Idempotent { get; }

        protected BaseMessageHandler()
        {
            _maxRetry = 2; // 按需修改
            _maxRequeue = 1;
            _backoffMs = 200;
            _requeueCounter = new Dictionary<string, int>();
            _tracker = new InMemoryRecvTracker();
        }

        // 说明：redelivered来自于上层传入的BasicDeliverEventArgs.Redelivered
        // 该值为false说明一定没有处理过该条消息，而为true则意义不大没办法说明任何事情
        public bool Handle(Message<TBody> message, bool redelivered)
        {
            // 说明：
            // 返回值true或false表达的是对于消息处理的业务结果，跟是否需要重新入队没有任何关系；
            // 库在这里的假设是如果DoHandle内部有异常情况发生则我们可以试着有限的requeue一波（仅针对特定的异常类型），
            // 因此你会看到DoRequeue的逻辑是放在catch(Exception<LightMessagerExceptionArgs>)处理块中的。
            // 这同时也意味着每一波retry之后是否重新入队我们都需要重新判断，msg.NeedRequeue正是用于此目的。
            // 注意bool值默认为false，如果没有任何异常则NeedRequeue自动为false。
            try
            {
                var ret = false;
                if (!redelivered && _tracker.TrackMessage(message))
                {
                    ret = RetryHelper.Retry(() => DoHandle(message), Idempotent ? _maxRetry : 1, _backoffMs);
                    _tracker.SetStatus(message, RecvStatus.Consumed);
                }
                return ret;
            }
            catch (Exception<LightMessagerExceptionArgs>)
            {
                DoRequeue(message);
            }
            catch (Exception ex)
            {
                var remark = $"未知异常：{ex.Message}；堆栈：{ex.StackTrace}";
                _tracker.SetStatus(message, RecvStatus.Failed, remark);
                _logger.Error(remark);
            }

            return false;
        }

        public async Task<bool> HandleAsync(Message<TBody> message, bool redelivered)
        {
            try
            {
                var ret = false;
                if (!redelivered && await _tracker.TrackMessageAsync(message))
                {
                    ret = await RetryHelper.RetryAsync(async () => await DoHandleAsync(message), Idempotent ? _maxRetry : 1, _backoffMs);
                    await _tracker.SetStatusAsync(message, RecvStatus.Consumed);
                }
                return ret;
            }
            catch (Exception<LightMessagerExceptionArgs>)
            {
                await DoRequeueAsync(message);
            }
            catch (Exception ex)
            {
                var remark = $"未知异常：{ex.Message}；堆栈：{ex.StackTrace}";
                await _tracker.SetStatusAsync(message, RecvStatus.Failed, remark);
                _logger.Error(remark);
            }

            return false;
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
            if (Idempotent)
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
                        _tracker.SetStatus(message, RecvStatus.Failed, remark);
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
            else
            {
                var remark = "消息处理端不支持幂等，消息不会requeue，处理结束";
                _tracker.SetStatus(message, RecvStatus.Failed, remark);
                _logger.Warn(remark);
            }
        }

        private async Task DoRequeueAsync(Message<TBody> message)
        {
            if (Idempotent)
            {
                var msgId = message.MsgId;
                if (_requeueCounter.TryGetValue(msgId, out int requeueCount))
                {
                    message.NeedRequeue = requeueCount < _maxRequeue;
                    if (message.NeedRequeue)
                    {
                        var newVal = requeueCount + 1;
                        _requeueCounter[msgId] = newVal;
                        _logger.Debug($"消息准备第[{newVal}]次requeue");
                    }
                    else
                    {
                        var remark = "消息requeue次数已达上限，不再尝试";
                        await _tracker.SetStatusAsync(message, RecvStatus.Failed, remark);
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
            else
            {
                var remark = "消息处理端不支持幂等，消息不会requeue，处理结束";
                await _tracker.SetStatusAsync(message, RecvStatus.Failed, remark);
                _logger.Warn(remark);
            }
        }
    }
}
