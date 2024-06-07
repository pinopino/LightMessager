using LightMessager.Model;

namespace LightMessager.Tracker
{
    public interface IMessageSendTracker
    {
        void OnAddMessageState(MessageSendState state);

        void OnSetMessageState(MessageSendState state);

        void OnModelShutdown(MessageSendState state);

        List<object> GetRetryItems();

        IAsyncEnumerable<object> GetRetryItemsAsync();
    }
}
