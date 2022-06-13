using LightMessager.Model;
using System.Threading.Tasks;

namespace LightMessager.Track
{
    public interface IMessageRecvTracker
    {
        bool TrackMessage(Message message);
        Task<bool> TrackMessageAsync(Message message);
        void SetStatus(Message message, RecvStatus newStatus, string remark = "");
        Task SetStatusAsync(Message message, RecvStatus newStatus, string remark = "");
    }
}
