using LightMessager.Model;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    public interface IMessageRecvTracker
    {
        bool TrackMessage(Message message);
        ValueTask<bool> TrackMessageAsync(Message message);
        void SetStatus(Message message, RecvStatus newStatus, string remark = "");
        ValueTask SetStatusAsync(Message message, RecvStatus newStatus, string remark = "");
    }
}
