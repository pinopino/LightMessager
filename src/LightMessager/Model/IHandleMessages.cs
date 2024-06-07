using System.Threading.Tasks;

namespace LightMessager.Model
{
    public interface IHandleMessages
    { }

    //
    // 摘要:
    //     Message handler interface. Implement this in order to get to handle messages
    //     of a specific type
    public interface IHandleMessages<TMessage> : IHandleMessages
    {
        bool Handle(TMessage message, bool redelivered);

        Task<bool> HandleAsync(TMessage message, bool redelivered);
    }
}
