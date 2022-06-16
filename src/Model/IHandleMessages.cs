using System.Threading.Tasks;

namespace LightMessager.Model
{
    public interface IHandleMessages
    { }

    //
    // 摘要:
    //     Message handler interface. Implement this in order to get to handle messages
    //     of a specific type
    public interface IHandleMessages<TBody> : IHandleMessages
    {
        bool Handle(Message<TBody> message);

        //
        // 摘要:
        //     This method will be invoked with a message of type TMessage
        Task<bool> HandleAsync(Message<TBody> message);
    }
}
