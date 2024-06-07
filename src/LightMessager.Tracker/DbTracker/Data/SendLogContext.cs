using Microsoft.EntityFrameworkCore;

namespace LightMessager.Tracker
{
    public class SendLogContext : DbContext
    {
        private readonly string _connectionString;

        public SendLogContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        public DbSet<SendLog> SendLogs { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseMySql(_connectionString, ServerVersion.AutoDetect(_connectionString));
        }

        protected override void OnModelCreating(ModelBuilder builder)
        {
            base.OnModelCreating(builder);

            builder.Entity<SendLog>().ToTable("send_log");
        }
    }
}
