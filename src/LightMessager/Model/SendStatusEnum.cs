namespace LightMessager.Model
{
    public enum SendStatus
    {
        PendingSend, // have not sent the message yet
        PendingResponse, // sent the message, waiting for an ack
        Success, // ack received
        Failed, // nack received
        Unroutable, // message returned
        NoExchangeFound // 404 reply code
    }
}
