namespace LightMessager.Model
{
    public enum SendStatus
    {
        PendingSend, // have not sent the message yet
        PendingResponse, // sent the message, waiting for an ack
        Confirmed, // ack received
        Nacked, // nack received
        Failed, // error
        Unroutable, // message returned
        NoExchangeFound // 404 reply code
    }
}
