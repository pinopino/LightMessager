using LightMessager;

namespace WebApplication1
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.

            builder.Services.AddControllers();
            // ע��RabbitMqHub��RabbitMqHub������ֻ��һ��������IConnection��װ
            // �������ｨ��ѡ��ע��ΪScoped
            builder.Services.AddScoped<RabbitMqHub>();

            var app = builder.Build();

            // Configure the HTTP request pipeline.

            app.UseHttpsRedirection();

            app.UseAuthorization();

            app.MapControllers();

            app.Run();
        }
    }
}
