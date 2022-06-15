namespace LightMessager.Model
{
    public enum SendStatus
    {
        PendingResponse, // sent the message, waiting for an ack
        Confirmed, // ack received
        Nacked, // nack received
        Failed, // error
        Unroutable, // message returned
        NoExchangeFound // 404 reply code
    }
}
