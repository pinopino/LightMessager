using LightMessager.Model;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    public interface IMessageSendTracker
    {
        void TrackMessage(ulong deliveryTag, Message message);
        ValueTask TrackMessageAsync(ulong deliveryTag, Message message);
        void SetStatus(ulong deliveryTag, SendStatus newStatus, string remark = "");
        ValueTask SetStatusAsync(ulong deliveryTag, SendStatus newStatus, string remark = "");
        void SetMultipleStatus(ulong deliveryTag, SendStatus newStatus);
        ValueTask SetMultipleStatusAsync(ulong deliveryTag, SendStatus newStatus);
        void SetStatus(Message message, SendStatus newStatus, string remark = "");
        ValueTask SetStatusAsync(Message message, SendStatus newStatus, string remark = "");
        void Reset(string remark = "");
        ValueTask ResetAsync();
    }
}
