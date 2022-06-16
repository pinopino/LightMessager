namespace LightMessager.Model
{
    public class MessageConsumeEventArgs
    {
        public ConsumeStatus ConsumeStatus { set; get; }
        public string MessageId { set; get; }
        public string MessageJson { set; get; }
        public string Remark { set; get; }
    }
}
