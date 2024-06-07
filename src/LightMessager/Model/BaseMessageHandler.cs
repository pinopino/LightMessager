using NLog;
using System.Threading.Tasks;

namespace LightMessager.Model
{
    public abstract class BaseMessageHandler<TMessage> : IHandleMessages<TMessage>
    {
        protected static Logger _logger = LogManager.GetLogger("MessageHandler");

        /// <summary>
        /// redelivered = BasicDeliverEventArgs.Redelivered + msgHeader["redelivered"]
        /// </summary>
        public virtual bool Handle(TMessage message, bool redelivered)
        {
            return true;
        }

        /// <summary>
        /// redelivered = BasicDeliverEventArgs.Redelivered + msgHeader["redelivered"]
        /// </summary>
        public virtual Task<bool> HandleAsync(TMessage message, bool redelivered)
        {
            return Task.FromResult(true);
        }
    }
}
