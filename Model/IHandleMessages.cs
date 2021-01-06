using System.Threading.Tasks;

namespace LightMessager.Model
{
    public interface IHandleMessages
    {
    }

    //
    // 摘要:
    //     Message handler interface. Implement this in order to get to handle messages
    //     of a specific type
    public interface IHandleMessages<in TMessage> : IHandleMessages 
        where TMessage : BaseMessage
    {
        void Handle(TMessage message);

        //
        // 摘要:
        //     This method will be invoked with a message of type TMessage
        Task HandleAsync(TMessage message);
    }
}
