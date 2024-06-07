using LightMessager.Model;
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace LightMessager.Tracker
{
    [Index("DeliveryTag", "ChannelId", "Status", Name = "idx_deliverytag_channelid_status")]
    [Index("Status", Name = "idx_status")]
    public class SendLog
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        [Column("message_id")]
        public long MessageId { set; get; }

        [Column("delivery_tag")]
        public ulong DeliveryTag { set; get; }

        [Column("message_pay_load")]
        [MaxLength(500)]
        public string? MessagePayload { set; get; }

        [Column("status")]
        public SendStatus Status { get; set; }

        [Column("acked")]
        public bool Acked { get; set; }

        [Column("multiple")]
        public bool? Multiple { get; set; }

        [Column("channel_id")]
        public long ChannelId { get; set; }

        [Column("remark")]
        [MaxLength(255)]
        public string? Remark { get; set; }

        [Column("updated_time")]
        public DateTime? UpdatedTime { set; get; }

        [Column("created_time")]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public DateTime CreatedTime { set; get; }
    }
}
