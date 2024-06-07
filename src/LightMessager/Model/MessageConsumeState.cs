namespace LightMessager.Model
{
    public class MessageConsumeState
    {
        public string MessagePayloadJson { get; internal set; }
        public ConsumeStatus Status { get; internal set; }
        public string Remark { get; internal set; }
    }
}
