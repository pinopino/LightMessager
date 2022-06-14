using LightMessager.Model;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    public interface IMessageSendTracker
    {
        Task TrackMessageAsync(ulong deliveryTag, Message message);
        Task SetStatusAsync(ulong deliveryTag, SendStatus newStatus, string remark = "");
        Task SetMultipleStatusAsync(ulong upToDeliveryTag, SendStatus newStatus, string remark = "");
        Task SetErrorAsync(Message message, SendStatus newStatus, string remark = "");
        Task ResetAsync(string remark = "");
    }
}
