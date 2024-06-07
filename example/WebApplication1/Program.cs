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
            // 注册RabbitMqHub，RabbitMqHub本质上只是一个轻量的IConnection封装
            // 所以这里建议选择注册为Scoped
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
