using LightMessager.Model;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;

namespace LightMessager.Tracker
{
    public class DbMessageTracker : IMessageSendTracker
    {
        private object _lockObj;
        private SendLogContext _dbContext;

        public DbMessageTracker(string connectionString)
        {
            _lockObj = new object();
            _dbContext = new SendLogContext(connectionString);
        }

        public void OnAddMessageState(MessageSendState state)
        {
            lock (_lockObj)
            {
                _dbContext.Add(new SendLog
                {
                    MessageId = state.MessageId,
                    DeliveryTag = state.SequenceNumber,
                    MessagePayload = JsonConvert.SerializeObject(state.MessagePayload),
                    Status = state.Status,
                    Acked = state.Acked,
                    Multiple = state.Multiple,
                    ChannelId = state.ChannelId,
                    Remark = state.Remark
                });
                _dbContext.SaveChanges();
            }
        }

        public void OnSetMessageState(MessageSendState state)
        {
            lock (_lockObj)
            {
                if (state.MessageId != 0)
                {
                    _dbContext.SendLogs
                        .Where(p => p.MessageId == state.MessageId &&
                                    p.Status != SendStatus.Unroutable)
                        .ExecuteUpdate(p =>
                                    p.SetProperty(p => p.Status, state.Status)
                                    .SetProperty(p => p.Remark, state.Remark)
                                    .SetProperty(p => p.UpdatedTime, DateTime.Now));
                }
                else
                {
                    if (state.Multiple)
                    {
                        _dbContext.SendLogs
                            .Where(p => p.DeliveryTag > 0 &&
                                        p.DeliveryTag <= state.SequenceNumber &&
                                        p.ChannelId == state.ChannelId &&
                                        p.Status == SendStatus.PendingResponse)
                            .ExecuteUpdate(p =>
                                        p.SetProperty(p => p.Status, state.Status)
                                        .SetProperty(p => p.Acked, state.Acked)
                                        .SetProperty(p => p.Remark, state.Remark)
                                        .SetProperty(p => p.UpdatedTime, DateTime.Now));
                    }
                    else
                    {
                        if (state.SequenceNumber > 0)
                        {
                            _dbContext.SendLogs
                                .Where(p => p.DeliveryTag == state.SequenceNumber &&
                                            p.ChannelId == state.ChannelId &&
                                            p.Status != SendStatus.Unroutable)
                                .ExecuteUpdate(p =>
                                            p.SetProperty(p => p.Status, state.Status)
                                            .SetProperty(p => p.Acked, state.Acked)
                                            .SetProperty(p => p.Remark, state.Remark)
                                            .SetProperty(p => p.UpdatedTime, DateTime.Now));
                        }
                        else
                        {
                            _dbContext.SendLogs
                                .Where(p => p.ChannelId == state.ChannelId &&
                                            p.Status != SendStatus.Unroutable)
                                .ExecuteUpdate(p =>
                                            p.SetProperty(p => p.Status, state.Status)
                                            .SetProperty(p => p.Acked, state.Acked)
                                            .SetProperty(p => p.Remark, state.Remark)
                                            .SetProperty(p => p.UpdatedTime, DateTime.Now));
                        }
                    }
                }
            }
        }

        public void OnModelShutdown(MessageSendState state)
        {
            throw new NotImplementedException();
        }

        public async IAsyncEnumerable<object> GetRetryItemsAsync()
        {
            var source = _dbContext.SendLogs
                        .Where(p => p.Status != SendStatus.Success);

            var pageIndex = 1;
            var pageSize = 200;
            while (true)
            {
                var page = await CreatePageAsync(source, pageIndex, pageSize);
                if (page.Count == 0)
                    break;

                foreach (var item in page)
                    yield return item.MessagePayload;

                if (page.HasNextPage)
                    pageIndex += 1;
                else
                    break;
            }
        }

        public List<object> GetRetryItems()
        {
            var list = _dbContext.SendLogs
                        .Where(p => p.Status != SendStatus.Success)
                        .AsNoTracking()
                        .Select(p => p.MessagePayload)
                        .AsEnumerable();

            return list.Select(p => (object)p).ToList();
        }

        private async Task<PaginatedList<SendLog>> CreatePageAsync(IQueryable<SendLog> source, int pageIndex = 0, int pageSize = 10)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (pageIndex < 0)
                throw new ArgumentException(nameof(pageIndex));
            if (pageSize <= 0)
                throw new ArgumentException(nameof(pageSize));

            var count = await source.CountAsync();
            var items = await source.Skip((pageIndex - 1) * pageSize)
                .Take(pageSize)
                .AsNoTracking()
                .ToListAsync();
            return new PaginatedList<SendLog>(items, count, pageIndex, pageSize);
        }
    }
}
